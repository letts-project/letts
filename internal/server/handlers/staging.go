package handlers

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"letts/internal/config"
	"letts/internal/fsutil"
	"letts/internal/ids"
	"letts/internal/metrics"
	"letts/internal/server/httputil"
	"letts/internal/stagingstore"
	"letts/internal/storage"
)

var sha256Regex = regexp.MustCompile(`^[a-f0-9]{64}$`)

// StagingHandler serves the staging endpoints. PUT/HEAD/GET/DELETE on
// individual staging_ids and the list / by-content lookup variants.
type StagingHandler struct {
	DB         *sql.DB
	Cfg        *config.DugdaleConfig
	DataDir    string
	UploadLock *stagingstore.UploadLock
	// Runtime delivers force-delete kills when DELETE ?force=true finds
	// referencing missions that are still running. Same contract as
	// LifecycleHandler.Runtime; nil makes such a force-delete fail with 500
	// rather than flipping a live row.
	Runtime LifecycleRuntime
	// ForceDeleteTimeout / ForceDeletePoll bound the finalize wait after a
	// force-delete kill (defaults 30s / 50ms, same as mission DELETE).
	ForceDeleteTimeout time.Duration
	ForceDeletePoll    time.Duration
	// DiskUsage gates PUT on data_dir size. When non-nil and the
	// cached usage exceeds cfg.Limits.MaxDataDirSize, PUT refuses with 503
	// disk_quota_exceeded before any bytes are accepted.
	DiskUsage func() int64
}

// Register mounts every staging route.
func (h *StagingHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("PUT /v1/staging/{id}", h.Put)
	mux.HandleFunc("HEAD /v1/staging/{id}", h.Head)
	mux.HandleFunc("GET /v1/staging/{id}", h.Get)
	mux.HandleFunc("DELETE /v1/staging/{id}", h.Delete)
	mux.HandleFunc("GET /v1/staging", h.List)
	mux.HandleFunc("GET /v1/staging/by-content/{sha}", h.ByContent)
}

// List implements GET /v1/staging?mission_id=...&ref_kind=...&limit=...&cursor=...
// Currently mission_id is required (the cleanup goroutine handles
// orphan listing on its own).
func (h *StagingHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	missionID := q.Get("mission_id")
	if missionID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "mission_id is required", nil)
		return
	}
	if !ids.ValidateUUIDv7(missionID) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid mission_id", nil)
		return
	}
	kindFilter := q.Get("ref_kind")
	switch kindFilter {
	case "", "input", "output":
		// ok (script not used in v1; future-compat)
	case "script":
		// future
	default:
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"ref_kind must be input or output", nil)
		return
	}

	limit := defaultListLimit
	if s := q.Get("limit"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request", "limit must be a positive integer", nil)
			return
		}
		if v > maxListLimit {
			v = maxListLimit
		}
		limit = v
	}
	cursor, err := decodeStagingCursor(q.Get("cursor"))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid cursor: "+err.Error(), nil)
		return
	}

	rows, err := storage.ListStagingByMission(r.Context(), h.DB, missionID, kindFilter, cursor, limit+1)
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.List/query", err)
		return
	}
	var nextCursor string
	if len(rows) > limit {
		last := rows[limit-1]
		nextCursor = encodeStagingCursor(&storage.StagingCursor{
			TimeCreatedMs: last.TimeCreatedMs, StagingID: last.StagingID,
		})
		rows = rows[:limit]
	}
	items := make([]map[string]any, 0, len(rows))
	for _, sf := range rows {
		items = append(items, map[string]any{
			"staging_id":     sf.StagingID,
			"state":          string(sf.State),
			"sha256":         sf.Sha256,
			"size":           sf.Size,
			"bytes_received": sf.BytesReceived,
			"time_created":   sf.TimeCreatedMs,
			"time_updated":   sf.TimeUpdatedMs,
			"time_expires":   sf.TimeExpiresMs,
			"ref_kind":       string(sf.RefKind),
			"role":           sf.Role,
		})
	}
	resp := map[string]any{"staging": items}
	if nextCursor != "" {
		resp["next_cursor"] = nextCursor
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// ByContent implements GET /v1/staging/by-content/{sha}?size=<bytes>.
// Looks up a complete staging row matching (sha256, size). The size param is
// mandatory: it disambiguates collisions and lets the server reject
// out-of-bounds requests early.
func (h *StagingHandler) ByContent(w http.ResponseWriter, r *http.Request) {
	sha := strings.ToLower(r.PathValue("sha"))
	if !sha256Regex.MatchString(sha) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "sha must be 64 lowercase hex chars", nil)
		return
	}
	sizeStr := r.URL.Query().Get("size")
	if sizeStr == "" {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "size is required", nil)
		return
	}
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil || size <= 0 {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "size must be a positive integer", nil)
		return
	}
	if max := h.stagingMaxUpload(); max > 0 && size > max {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"size exceeds max_staging_upload_size", map[string]any{"size": size, "max": max})
		return
	}

	sf, err := storage.LookupStagingByContent(r.Context(), h.DB, sha, size)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "no complete staging file matches sha+size", nil)
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.ByContent/lookup", err)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"staging_id": sf.StagingID,
		"sha256":     sf.Sha256,
		"size":       sf.Size,
	})
}

func encodeStagingCursor(c *storage.StagingCursor) string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeStagingCursor(s string) (*storage.StagingCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c storage.StagingCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Delete implements DELETE /v1/staging/{id}. Without force=true a
// referenced staging file returns 409 staging_in_use listing the dependent
// mission ids. With force=true the dependent missions are cascade-deleted:
// running ones are first retired via the force-delete kill and bounded
// finalize wait (the same machinery as DELETE /v1/missions/{id}?force=true,
// including its 504 when the process won't die in time), then queued/done
// ones are flipped to deleting (cleanup goroutine handles file/row removal)
// and the staging row itself is marked deleting.
func (h *StagingHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ids.ValidateUUIDv7(id) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid staging_id", nil)
		return
	}
	force := strings.EqualFold(r.URL.Query().Get("force"), "true")

	sf, err := storage.GetStaging(r.Context(), h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "staging not found", nil)
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Delete/get_staging", err)
		return
	}
	if sf.State == storage.StagingDeleting {
		httputil.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "deletion_pending"})
		return
	}

	// Cheap read OUTSIDE the writer transaction: enough to answer the common
	// non-force 409 without taking the write lock, and to seed the force
	// path's kill pass. NOT authoritative — dispatch inserts refs inside its
	// own writer transaction, so the actual delete decision is re-made on a
	// re-read INSIDE the transaction below.
	refs, err := storage.RefsByStaging(r.Context(), h.DB, id)
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Delete/refs_by_staging", err)
		return
	}
	if len(refs) > 0 && !force {
		writeStagingInUse(w, distinctMissionIDs(refs))
		return
	}

	if force {
		// Kill pass: every referencing mission currently running is retired
		// (force-delete kill and bounded finalize wait) BEFORE anything flips
		// to deleting. Flipping a live running row would hand it to the
		// cleanup goroutine, which removes the mission row and files from
		// under the still-running process. The status-guarded terminal
		// commit makes a late finalize land harmlessly once a row is
		// deleting — the kill is what actually stops the process.
		for _, mid := range distinctMissionIDs(refs) {
			m, err := storage.GetMission(r.Context(), h.DB, mid)
			if errors.Is(err, storage.ErrNotFound) {
				continue // mission hard-deleted concurrently; its refs went with it
			}
			if err != nil {
				httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Delete/get_mission", err)
				return
			}
			if m.Status != storage.StatusRunning {
				continue
			}
			if out := forceKillAndAwaitFinalize(r.Context(), h.DB, h.Runtime, mid,
				h.ForceDeleteTimeout, h.ForceDeletePoll); out.Status != 0 {
				httputil.WriteError(w, out.Status, out.ErrorCode, out.ErrorMsg, out.Details)
				return
			}
		}
	}

	// One writer transaction makes the decision authoritative: refs are
	// re-read on the pinned conn, so a dispatch committing a new ref
	// concurrently cannot slip past the check, and a queued ref that a lane
	// runner picked up between the kill pass and this transaction is caught
	// while everything is still un-flipped.
	var (
		conflictMissions []string // distinct refs at txn time, for the 409 body
		stillRunning     bool     // force only: a referencing mission runs at txn time
		cascadeCount     int      // distinct referencing missions at txn time (already-deleting ones included), for the audit line
	)
	err = storage.WithWriter(r.Context(), h.DB, func(c *sql.Conn) error {
		txRefs, err := storage.RefsByStaging(r.Context(), c, id)
		if err != nil {
			return err
		}
		missions := distinctMissionIDs(txRefs)
		if !force {
			if len(missions) > 0 {
				conflictMissions = missions
				return errStagingDeleteConflict
			}
		} else {
			for _, mid := range missions {
				var status string
				err := c.QueryRowContext(r.Context(),
					`SELECT status FROM missions WHERE mission_id=?`, mid).Scan(&status)
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return err
				}
				if status == string(storage.StatusRunning) {
					conflictMissions = append(conflictMissions, mid)
				}
			}
			if len(conflictMissions) > 0 {
				stillRunning = true
				return errStagingDeleteConflict
			}
			for _, mid := range missions {
				// Queued rows flip directly: deletion removes the record, so
				// no terminal event is owed (same contract as mission
				// DELETE). Running rows were excluded above; deleting rows
				// are already on their way out.
				if _, err := c.ExecContext(r.Context(),
					`UPDATE missions SET status='deleting'
					 WHERE mission_id=? AND status IN ('queued','done')`, mid); err != nil {
					return err
				}
			}
			cascadeCount = len(missions)
		}
		return storage.MarkStagingDeleting(r.Context(), c, id)
	})
	switch {
	case errors.Is(err, errStagingDeleteConflict):
		if stillRunning {
			// force=true raced a lane runner: a referencing mission entered
			// 'running' after the kill pass. Same 409 staging_in_use shape
			// as the non-force conflict; a retry re-runs the kill pass
			// against the now-running set.
			httputil.WriteError(w, http.StatusConflict, "staging_in_use",
				"a referencing mission started running during force-delete; retry to kill it",
				map[string]any{"missions": conflictMissions})
			return
		}
		writeStagingInUse(w, conflictMissions)
		return
	case errors.Is(err, storage.ErrStagingFinalizing):
		// Phase B owns pending_output/committing rows. Admin can
		// retry once finalize completes (typically < 10ms).
		httputil.WriteError(w, http.StatusConflict, "staging_finalizing",
			"staging file is mid output-commit; retry after current finalize completes",
			nil)
		return
	case err != nil:
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Delete/mark_deleting", err)
		return
	}
	// Audit log is mandated for destructive admin ops; cascade-deleting
	// referenced missions with force=true would otherwise be invisible to
	// operators.
	auditLog(nil, r, "staging.delete",
		"staging_id", id, "force", force, "cascade_missions", cascadeCount)
	httputil.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "deletion_pending"})
}

// errStagingDeleteConflict aborts the staging-delete transaction when refs
// exist (non-force) or a referencing mission is running (force) at
// transaction time. Carries no payload; the handler builds the 409 from the
// mission list captured alongside it.
var errStagingDeleteConflict = errors.New("staging delete conflict")

// writeStagingInUse emits the canonical 409 staging_in_use response.
func writeStagingInUse(w http.ResponseWriter, missions []string) {
	httputil.WriteError(w, http.StatusConflict, "staging_in_use",
		"staging file is referenced by missions; use ?force=true to cascade-delete",
		map[string]any{"missions": missions})
}

// distinctMissionIDs returns the unique mission ids referenced by refs, in
// first-seen order.
func distinctMissionIDs(refs []storage.StagingRef) []string {
	seen := make(map[string]bool, len(refs))
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if !seen[ref.MissionID] {
			seen[ref.MissionID] = true
			out = append(out, ref.MissionID)
		}
	}
	return out
}

// Get implements GET /v1/staging/{id}. Uses http.ServeContent to
// handle Range requests (206 partial) and emits a 200 Last-Modified-aware
// full body otherwise. After a successful full download (status 200), the
// row's downloaded_at is set so the cleanup goroutine can apply
// downloaded_grace if/when the artifact becomes orphan.
func (h *StagingHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ids.ValidateUUIDv7(id) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid staging_id", nil)
		return
	}
	sf, err := storage.GetStaging(r.Context(), h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "staging not found", nil)
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Get/get_staging", err)
		return
	}
	switch sf.State {
	case storage.StagingComplete:
		// proceed
	case storage.StagingUploading:
		httputil.WriteError(w, http.StatusConflict, "staging_uploading",
			"upload in progress; cannot download yet",
			map[string]any{"bytes_received": sf.BytesReceived, "total_size": sf.Size})
		return
	case storage.StagingDeleting:
		httputil.WriteError(w, http.StatusGone, "deleting", "staging is being deleted", nil)
		return
	default:
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			"unknown staging state: "+string(sf.State), nil)
		return
	}

	abs := filepath.Join(h.DataDir, sf.Path)
	f, err := os.Open(abs)
	if errors.Is(err, os.ErrNotExist) {
		// The GC may have renamed staging/<sh>/<sh>/<id> → tombstone/<id> in
		// the window between our row read (still 'complete') and this open.
		// During the tombstone grace the bytes are still there; serve them so
		// a late download isn't broken by a spurious error. Past
		// grace the tombstone is unlinked and we fall through to ENOENT → 410.
		tomb := filepath.Join(h.DataDir, "tombstone", id)
		f, err = os.Open(tomb)
	}
	if err != nil {
		httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Get/open", err)
		return
	}
	defer func() { _ = f.Close() }()

	sc := &statusCapturingWriter{ResponseWriter: w, status: http.StatusOK}
	sc.Header().Set("Content-Type", "application/octet-stream")
	sc.Header().Set("X-Letts-Sha256", sf.Sha256)

	modTime := time.UnixMilli(sf.TimeUpdatedMs)
	http.ServeContent(sc, r, sf.StagingID, modTime, f)

	// Mark downloaded_at on full GETs (status 200) only — first time only.
	if sc.status == http.StatusOK && !sf.DownloadedAt.Valid {
		now := time.Now().UnixMilli()
		_, _ = h.DB.ExecContext(context.Background(),
			`UPDATE staging_files SET downloaded_at=? WHERE staging_id=? AND downloaded_at IS NULL`,
			now, id)
	}
}

// statusCapturingWriter records the first WriteHeader call so the GET handler
// can branch on the actual status code returned by http.ServeContent (200 vs
// 206 vs 416 etc.) without parsing the response.
type statusCapturingWriter struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *statusCapturingWriter) WriteHeader(code int) {
	if !w.written {
		w.status = code
		w.written = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusCapturingWriter) Write(p []byte) (int, error) {
	if !w.written {
		w.status = http.StatusOK
		w.written = true
	}
	return w.ResponseWriter.Write(p)
}

// Head implements HEAD /v1/staging/{id}. Returns custom X-Letts-*
// headers describing upload state so resume clients know what offset to
// continue from.
func (h *StagingHandler) Head(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ids.ValidateUUIDv7(id) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	sf, err := storage.GetStaging(r.Context(), h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Letts-Sha256", sf.Sha256)
	switch sf.State {
	case storage.StagingComplete:
		w.Header().Set("Content-Length", strconv.FormatInt(sf.Size, 10))
		w.Header().Set("X-Letts-Upload-Status", "complete")
	case storage.StagingUploading:
		w.Header().Set("X-Letts-Upload-Status", "incomplete")
		w.Header().Set("X-Letts-Bytes-Received", strconv.FormatInt(sf.BytesReceived, 10))
		w.Header().Set("X-Letts-Total-Size", strconv.FormatInt(sf.Size, 10))
	case storage.StagingDeleting:
		w.Header().Set("X-Letts-Upload-Status", "deleting")
	default:
		w.Header().Set("X-Letts-Upload-Status", string(sf.State))
	}
	w.WriteHeader(http.StatusOK)
}

// stagingTTLMs returns the configured staging TTL in milliseconds, falling
// back to 1h if cfg is missing or zeroed.
func (h *StagingHandler) stagingTTLMs() int64 {
	if h.Cfg == nil || h.Cfg.Cleanup.StagingTTL <= 0 {
		return int64(time.Hour / time.Millisecond)
	}
	return h.Cfg.Cleanup.StagingTTL.Milliseconds()
}

// stagingMaxUpload returns max_staging_upload_size or 0 (no limit).
func (h *StagingHandler) stagingMaxUpload() int64 {
	if h.Cfg == nil {
		return 0
	}
	return h.Cfg.Limits.MaxStagingUploadSize
}

// uploadIdleTimeout returns the configured upload_idle_timeout (the same
// limit the UploadLock janitor sweeps on) or 0 when unset, which disables
// the per-request read deadline.
func (h *StagingHandler) uploadIdleTimeout() time.Duration {
	if h.Cfg == nil {
		return 0
	}
	return h.Cfg.Limits.UploadIdleTimeout
}

// enforceIncompleteLimits checks the incomplete-upload caps.
// Returns a descriptive error when the new PUT would push
// the daemon past max_incomplete_staging_uploads (count of `state=
// 'uploading'` rows) or max_incomplete_staging_bytes (sum of
// bytes_received over those rows). Both default-unlimited if config
// is missing or set to 0; only the count has a non-zero default (128
// per applyDefaults).
func (h *StagingHandler) enforceIncompleteLimits(ctx context.Context) error {
	if h.Cfg == nil {
		return nil
	}
	maxCount := h.Cfg.Limits.MaxIncompleteUploads
	maxBytes := h.Cfg.Limits.MaxIncompleteBytes
	if maxCount <= 0 && maxBytes <= 0 {
		return nil
	}
	var n int
	var sum int64
	err := h.DB.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(bytes_received), 0)
		 FROM staging_files WHERE state='uploading'`).Scan(&n, &sum)
	if err != nil {
		return fmt.Errorf("count incomplete uploads: %w", err)
	}
	if maxCount > 0 && n >= maxCount {
		return fmt.Errorf("max_incomplete_staging_uploads exceeded (%d/%d in-flight)", n, maxCount)
	}
	if maxBytes > 0 && sum >= maxBytes {
		return fmt.Errorf("max_incomplete_staging_bytes exceeded (%d/%d bytes in-flight)", sum, maxBytes)
	}
	return nil
}

// parseContentRange parses an HTTP "Content-Range: bytes <start>-<end>/<total>"
// value, validating relative ordering.
func parseContentRange(cr string) (start, end, total int64, err error) {
	cr = strings.TrimSpace(cr)
	if !strings.HasPrefix(cr, "bytes ") {
		return 0, 0, 0, errors.New("missing 'bytes ' prefix")
	}
	body := strings.TrimPrefix(cr, "bytes ")
	parts := strings.SplitN(body, "/", 2)
	if len(parts) != 2 {
		return 0, 0, 0, errors.New("missing '/total'")
	}
	rng := strings.SplitN(parts[0], "-", 2)
	if len(rng) != 2 {
		return 0, 0, 0, errors.New("malformed range")
	}
	if start, err = strconv.ParseInt(rng[0], 10, 64); err != nil {
		return 0, 0, 0, fmt.Errorf("start: %w", err)
	}
	if end, err = strconv.ParseInt(rng[1], 10, 64); err != nil {
		return 0, 0, 0, fmt.Errorf("end: %w", err)
	}
	if total, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
		return 0, 0, 0, fmt.Errorf("total: %w", err)
	}
	if start < 0 || end < start || total <= 0 || end >= total {
		return 0, 0, 0, errors.New("invalid range values")
	}
	return start, end, total, nil
}

// Put implements PUT /v1/staging/{id}.
//
// Branches:
//   - existing complete + sha/size match  → 200, body NOT consumed (Connection: close)
//   - existing complete + sha/size differ → 409 content_mismatch
//   - existing uploading + sha mismatch   → 409 content_mismatch
//   - existing uploading + range mismatch → 416 (+ HEAD-required hint)
//   - existing uploading + match          → resume: re-hash prefix from disk, append
//   - no row + start != 0                 → 416 (no upload started)
//   - no row + start == 0                 → insert + stream
func (h *StagingHandler) Put(w http.ResponseWriter, r *http.Request) {
	// Content-Encoding compression is explicitly
	// disallowed on PUT /v1/staging/{id} because the upload has byte-offset
	// semantics (resumable Content-Range, sha256-of-raw-bytes). Reject up
	// front so clients fail loudly rather than producing a sha mismatch
	// after streaming.
	if ce := strings.TrimSpace(r.Header.Get("Content-Encoding")); ce != "" && !strings.EqualFold(ce, "identity") {
		httputil.WriteError(w, http.StatusUnsupportedMediaType, "unsupported_media_type",
			"Content-Encoding not supported on staging PUT (byte-offset semantics)",
			map[string]any{"content_encoding": ce})
		return
	}
	// Quota gate: refuse PUT before any bytes are accepted when
	// data_dir size already exceeds max_data_dir_size.
	if h.Cfg.Limits.MaxDataDirSize > 0 && h.DiskUsage != nil &&
		h.DiskUsage() > h.Cfg.Limits.MaxDataDirSize {
		w.Header().Set("Retry-After", "30")
		httputil.WriteError(w, http.StatusServiceUnavailable, "disk_quota_exceeded",
			"data_dir size exceeds max_data_dir_size", nil)
		return
	}
	id := r.PathValue("id")
	if !ids.ValidateUUIDv7(id) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid staging_id", nil)
		return
	}
	declared := strings.ToLower(r.Header.Get("X-Letts-Sha256"))
	if !sha256Regex.MatchString(declared) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"X-Letts-Sha256 must be 64 lowercase hex chars", nil)
		return
	}

	var rangeStart, total int64
	hasContentRange := false
	var crEnd int64
	if cr := r.Header.Get("Content-Range"); cr != "" {
		s, e, t, err := parseContentRange(cr)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request",
				"invalid Content-Range: "+err.Error(), nil)
			return
		}
		rangeStart, crEnd, total = s, e, t
		hasContentRange = true
	} else {
		if r.ContentLength < 0 {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request",
				"Content-Length required for initial PUT", nil)
			return
		}
		total = r.ContentLength
	}
	if total <= 0 {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "declared total must be > 0", nil)
		return
	}
	// When Content-Range is present, its end must name the last byte of THIS
	// chunk: end == start + ContentLength - 1. The write loop is driven off the
	// denominator (total), so without this check a client could claim a small
	// chunk (e.g. bytes 0-2) while streaming the whole file, breaking the
	// per-chunk byte-offset contract the resumable protocol relies on.
	if hasContentRange && r.ContentLength >= 0 && crEnd != rangeStart+r.ContentLength-1 {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			fmt.Sprintf("Content-Range end %d does not match chunk length %d (start %d)",
				crEnd, r.ContentLength, rangeStart), nil)
		return
	}
	if max := h.stagingMaxUpload(); max > 0 && total > max {
		httputil.WriteError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
			fmt.Sprintf("declared total %d exceeds max_staging_upload_size %d", total, max), nil)
		return
	}
	// A declared total that alone exceeds max_data_dir_size can never complete
	// in this data_dir — reject up front instead of streaming until
	// a mid-stream quota re-check trips. This is a static comparison and needs
	// no DiskUsage probe.
	if h.Cfg != nil && h.Cfg.Limits.MaxDataDirSize > 0 && total > h.Cfg.Limits.MaxDataDirSize {
		w.Header().Set("Retry-After", "30")
		httputil.WriteError(w, http.StatusServiceUnavailable, "disk_quota_exceeded",
			fmt.Sprintf("declared total %d exceeds max_data_dir_size %d", total, h.Cfg.Limits.MaxDataDirSize), nil)
		return
	}

	// Per-id upload lock with idle-abort tied to request ctx.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	// On idle-abort, flip the staging row's time_expires to
	// "now" so the cleanup GC sweeps the partial file on its NEXT cycle
	// instead of holding it for the full staging_ttl. Without this,
	// abandoned slowloris-style uploads keep disk and count budget pinned
	// until staging_ttl elapses (typically 1h+).
	rel, ok := h.UploadLock.TryAcquire(id, h.idleAbortFn(id, cancel))
	if !ok {
		httputil.WriteError(w, http.StatusConflict, "upload_in_progress",
			"another upload is active for this staging_id", nil)
		return
	}
	defer rel()

	existing, err := storage.GetStaging(ctx, h.DB, id)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Put/get_existing", err)
		return
	}
	if errors.Is(err, storage.ErrNotFound) {
		existing = nil
	}

	shard, _ := ids.ShardPath(id)
	relPath := filepath.Join("staging", shard, id)
	abs := filepath.Join(h.DataDir, relPath)

	if existing != nil {
		if existing.Sha256 != declared || existing.Size != total {
			httputil.WriteError(w, http.StatusConflict, "content_mismatch",
				"declared sha/size differs from existing staging row",
				map[string]any{"existing_sha256": existing.Sha256, "existing_size": existing.Size})
			return
		}
		switch existing.State {
		case storage.StagingComplete:
			// Already complete — don't consume body.
			w.Header().Set("Connection", "close")
			httputil.WriteJSON(w, http.StatusOK, map[string]any{
				"staging_id":  id,
				"sha256":      declared,
				"size":        total,
				"ttl_seconds": int(h.stagingTTLMs() / 1000),
			})
			return
		case storage.StagingUploading:
			// Verify partial file matches bytes_received before continuing.
			if err := verifyPartialFile(abs, existing.BytesReceived); err != nil {
				httputil.WriteError(w, http.StatusRequestedRangeNotSatisfiable,
					"range_not_satisfiable", err.Error(), nil)
				return
			}
			if rangeStart != existing.BytesReceived {
				httputil.WriteError(w, http.StatusRequestedRangeNotSatisfiable,
					"range_not_satisfiable",
					"Content-Range start != bytes_received; HEAD first to resync",
					map[string]any{"bytes_received": existing.BytesReceived})
				return
			}
		default:
			httputil.WriteError(w, http.StatusConflict, "conflict",
				"staging in invalid state for upload",
				map[string]any{"state": string(existing.State)})
			return
		}
	} else {
		if rangeStart != 0 {
			httputil.WriteError(w, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable",
				"no upload started for this staging_id; HEAD first", nil)
			return
		}
	}

	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Put/mkdir", err)
		return
	}
	// fsync each newly-created level of staging/<sh1>/<sh2>. Without
	// this, MkdirAll creating both <sh1> and <sh2> in one call leaves
	// the <sh2> entry inside <sh1> on a dirty dir page; a later
	// SyncDir on the leaf only flushes <sh2>'s contents, not the
	// <sh2> entry itself. Failures logged+counted.
	metrics.ObserveSyncDir(
		fsutil.SyncDirChain(filepath.Join(h.DataDir, "staging"), shard),
		nil, "staging_put_outdir")

	nowMs := time.Now().UnixMilli()
	if existing == nil {
		// Incomplete-upload limits: count and bytes
		// of currently-uploading rows. Cap'd via dugdale.yaml
		// limits.max_incomplete_staging_uploads (default 128) and
		// limits.max_incomplete_staging_bytes (default 0 = unlimited).
		// Without this guard, an attacker (or buggy client) can open N
		// slow PUTs of huge declared totals and only UploadIdleTimeout
		// saves the daemon — that's a per-upload timer, not a global
		// budget.
		if err := h.enforceIncompleteLimits(ctx); err != nil {
			httputil.WriteError(w, http.StatusServiceUnavailable, "incomplete_uploads_full",
				err.Error(), nil)
			return
		}
		if err := storage.InsertStaging(ctx, h.DB, &storage.StagingFile{
			StagingID:     id,
			State:         storage.StagingUploading,
			Sha256:        declared,
			Size:          total,
			BytesReceived: 0,
			Path:          relPath,
			TimeCreatedMs: nowMs,
			TimeUpdatedMs: nowMs,
			TimeExpiresMs: nowMs + h.stagingTTLMs(),
		}); err != nil {
			httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Put/insert_staging", err)
			return
		}
	}

	// Open file for write.
	openMode := os.O_WRONLY
	if existing == nil {
		openMode |= os.O_CREATE | os.O_TRUNC
	} else {
		openMode |= os.O_APPEND
	}
	f, err := os.OpenFile(abs, openMode, 0o600)
	if err != nil {
		httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Put/open", err)
		return
	}
	defer func() { _ = f.Close() }()

	// For resume, re-hash existing prefix from disk so the final hasher
	// reflects the full content.
	hasher := sha256.New()
	written := int64(0)
	if existing != nil && existing.BytesReceived > 0 {
		rf, err := os.Open(abs)
		if err != nil {
			httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Put/open_for_hash", err)
			return
		}
		if _, err := io.Copy(hasher, rf); err != nil {
			_ = rf.Close()
			httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Put/hash_prefix", err)
			return
		}
		_ = rf.Close()
		written = existing.BytesReceived
	}

	// Idle defence for the body stream itself. The janitor's child-ctx
	// cancel cannot unblock a goroutine parked in r.Body.Read (the read is
	// serviced by the connection, and the server sets only
	// ReadHeaderTimeout), so a client that opens a PUT and goes silent would
	// pin this goroutine and fd forever. A per-request read deadline, pushed
	// forward after every accepted chunk, turns that silence into a read
	// error we can answer. Armed here — at the start of the body-read phase
	// — so earlier returns (200 replay, 416, 409) never touch the
	// connection's deadline. When the ResponseWriter doesn't support
	// deadlines (e.g. tests against a ResponseRecorder) we keep today's
	// janitor-only behavior rather than failing the upload.
	idle := h.uploadIdleTimeout()
	rc := http.NewResponseController(w)
	deadlineArmed := false
	if idle > 0 && rc.SetReadDeadline(time.Now().Add(idle)) == nil {
		deadlineArmed = true
	}

	buf := make([]byte, 64*1024)
	// Periodic quota re-check during streaming append.
	// Without this a multi-GB PUT that started
	// under MaxDataDirSize can keep writing past the quota — ENOSPC
	// eventually saves us but valid dispatches/uploads parallel to
	// this one are wrongly 503'd in the meantime, and we exceed the
	// soft cap. Check every ~16 MiB so the overhead is negligible.
	const quotaCheckInterval = int64(16 << 20)
	var bytesSinceQuotaCheck int64
	for written < total {
		n, rerr := r.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if int64(n) > total-written {
				chunk = chunk[:total-written]
				n = len(chunk)
			}
			if _, werr := f.Write(chunk); werr != nil {
				httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Put/write", werr)
				return
			}
			hasher.Write(chunk)
			written += int64(n)
			bytesSinceQuotaCheck += int64(n)
			h.UploadLock.Touch(id)
			if deadlineArmed {
				// Progress: push the idle deadline forward, mirroring the
				// janitor's Touch above.
				_ = rc.SetReadDeadline(time.Now().Add(idle))
			}
			now := time.Now().UnixMilli()
			_ = storage.UpdateStagingProgress(ctx, h.DB, id, written, now, now+h.stagingTTLMs())

			// Quota re-check. Only if max_data_dir_size is set and
			// DiskUsage is wired. Abort, mark deleting, and 503 when
			// usage crosses the cap mid-stream.
			if bytesSinceQuotaCheck >= quotaCheckInterval && h.Cfg != nil &&
				h.Cfg.Limits.MaxDataDirSize > 0 && h.DiskUsage != nil {
				bytesSinceQuotaCheck = 0
				if h.DiskUsage() >= h.Cfg.Limits.MaxDataDirSize {
					// Use context.Background for the DB
					// write — the request ctx may be torn down mid
					// 503-response and lose the MarkStagingDeleting
					// transaction, leaving the row stuck uploading
					// while os.Remove already wiped the file. Order:
					// DB flip first, file removal after.
					_ = storage.MarkStagingDeleting(context.Background(), h.DB, id)
					_ = os.Remove(abs)
					httputil.WriteError(w, http.StatusServiceUnavailable,
						"disk_quota_exceeded",
						"data_dir size limit reached during upload; partial discarded",
						map[string]any{"max_data_dir_size": h.Cfg.Limits.MaxDataDirSize})
					return
				}
			}
		}
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			if errors.Is(rerr, os.ErrDeadlineExceeded) {
				// Idle abort: no body progress within upload_idle_timeout.
				// Same ordering as the mid-stream quota abort above —
				// durable DB flip first on a context that survives the
				// response teardown, file removal after. Idempotent against
				// the janitor firing for the same upload: MarkStagingDeleting
				// is state-guarded (uploading/complete only; already-deleting
				// is a no-op), and the janitor itself only rewrites
				// time_expires on rows still 'uploading'.
				_ = storage.MarkStagingDeleting(context.Background(), h.DB, id)
				_ = os.Remove(abs)
				httputil.WriteError(w, http.StatusRequestTimeout, "upload_idle_timeout",
					"no upload progress within upload_idle_timeout; partial discarded — restart the upload",
					nil)
				return
			}
			httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Put/read", rerr)
			return
		}
	}
	// Past this point the body has been consumed up to `total` (or EOF), so
	// the connection may be reused for a keep-alive follow-up — clear the
	// per-request deadline so the NEXT request doesn't inherit one anchored
	// at our last chunk. This single clear covers every post-loop exit (201,
	// 400 incomplete_upload, 409 content_mismatch, the 500s). The in-loop
	// error returns above skip it on purpose: they leave body bytes unread,
	// so the server tears the connection down (or deadline-fails its drain)
	// instead of reusing it.
	if deadlineArmed {
		_ = rc.SetReadDeadline(time.Time{})
	}
	if err := f.Sync(); err != nil {
		httputil.WriteIOError(w, http.StatusInternalServerError, "staging.Put/fsync", err)
		return
	}
	// Final fsync of the dir holding the uploaded staging file. Without
	// this, the file is renamed into <sh1>/<sh2> but the entry isn't
	// durable; a crash can resurrect a phantom row.
	metrics.ObserveSyncDir(
		fsutil.SyncDir(filepath.Dir(abs)),
		nil, "staging_upload_complete")

	if written != total {
		// Body shorter than declared — keep row in uploading state for retry.
		httputil.WriteError(w, http.StatusBadRequest, "incomplete_upload",
			fmt.Sprintf("body shorter than declared total (got %d, want %d)", written, total),
			map[string]any{"bytes_received": written})
		return
	}

	actual := hex.EncodeToString(hasher.Sum(nil))
	if actual != declared {
		// Same ordering: durable mark deleting before file removal,
		// and use a context that survives the response close.
		_ = storage.MarkStagingDeleting(context.Background(), h.DB, id)
		_ = os.Remove(abs)
		httputil.WriteError(w, http.StatusConflict, "content_mismatch",
			fmt.Sprintf("computed sha256 %s does not match declared %s", actual, declared),
			nil)
		return
	}
	completedMs := time.Now().UnixMilli()
	// Use context.Background, not the request ctx: the body is fully read and
	// the file is durable on disk, so a client disconnect (or idle-abort
	// cancel) in this window must not abort the completion write — otherwise
	// the row stays 'uploading' while the complete, verified file sits on disk
	// (a silently lost upload). Mirrors the mismatch/quota-abort paths above.
	if err := storage.MarkStagingComplete(context.Background(), h.DB, id, actual, total,
		completedMs, completedMs+h.stagingTTLMs()); err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "staging.Put/mark_complete", err)
		return
	}
	httputil.WriteJSON(w, http.StatusCreated, map[string]any{
		"staging_id":  id,
		"sha256":      actual,
		"size":        total,
		"ttl_seconds": int(h.stagingTTLMs() / 1000),
	})
}

// verifyPartialFile checks that the on-disk size of a resuming upload matches
// the row's bytes_received. Returns nil on match.
func verifyPartialFile(abs string, expected int64) error {
	st, err := os.Stat(abs)
	if errors.Is(err, os.ErrNotExist) {
		if expected == 0 {
			return nil
		}
		return errors.New("partial file missing; HEAD first to resync")
	}
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if st.Size() != expected {
		return fmt.Errorf("partial file size %d != bytes_received %d; HEAD first", st.Size(), expected)
	}
	return nil
}

// idleAbortFn returns the onIdle callback for an upload of id. On janitor-
// triggered idle abort it flips the staging row's time_expires to "now"
// (so cleanup picks the partial file up next cycle) AND cancels the child
// request context (aborting the per-chunk DB updates).
//
// This is the second line of idle defence. The primary one is the
// per-request read deadline armed by the PUT loop — a context cancel cannot
// unblock a goroutine parked in r.Body.Read. The janitor still earns its
// keep when the ResponseWriter doesn't support deadlines and for reclaiming
// the disk/count budget when no read is in flight. Both paths may fire for
// the same upload; each side's SQL is state-guarded, so the overlap is
// benign.
func (h *StagingHandler) idleAbortFn(id string, cancel context.CancelFunc) func() {
	return func() {
		nowMs := time.Now().UnixMilli()
		// Background ctx — request ctx is about to be canceled, can't use it.
		_, _ = h.DB.ExecContext(context.Background(),
			`UPDATE staging_files SET time_expires=?, time_updated=?
			 WHERE staging_id=? AND state='uploading'`,
			nowMs, nowMs, id)
		cancel()
	}
}
