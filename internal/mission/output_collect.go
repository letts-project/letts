package mission

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"letts/internal/config"
	"letts/internal/fsutil"
	"letts/internal/ids"
)

// missionCollectQuota builds a CollectQuota from a *config.DugdaleConfig
// when both DiskUsage and MaxDataDirSize are wired; otherwise returns nil
// (no quota check). Shared by the two call sites (waiter, exec_runtime).
func missionCollectQuota(cfg *config.DugdaleConfig) *CollectQuota {
	if cfg == nil || cfg.DiskUsage == nil || cfg.Limits.MaxDataDirSize <= 0 {
		return nil
	}
	return &CollectQuota{
		DiskUsage:      cfg.DiskUsage,
		MaxDataDirSize: cfg.Limits.MaxDataDirSize,
	}
}

// Sentinel errors copyWithQuota returns so callers can distinguish the
// soft-quota abort from a generic IO failure and from per-file-too-large.
var (
	errDataDirQuotaExceeded = errors.New("data_dir_quota_exceeded")
	errOutputTooLarge       = errors.New("output_too_large")
)

// copyWithQuota copies src→dst, enforcing the per-file maxFileSize cap
// (when > 0) AND the data_dir soft-cap (when quota is non-nil and wired).
// Returns bytesCopied and either io.EOF (success), errOutputTooLarge,
// errDataDirQuotaExceeded, or the underlying IO error.
func copyWithQuota(dst io.Writer, src io.Reader, maxFileSize int64, quota *CollectQuota) (int64, error) {
	buf := make([]byte, 64*1024)
	var total, sinceQuota int64
	quotaEnabled := quota != nil && quota.MaxDataDirSize > 0 && quota.DiskUsage != nil
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			// Per-file cap check before writing — if writing this chunk
			// would push us past the cap, fail without persisting it.
			if maxFileSize > 0 && total+int64(n) > maxFileSize {
				return total, errOutputTooLarge
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
			sinceQuota += int64(n)

			if quotaEnabled && sinceQuota >= quotaCheckInterval {
				sinceQuota = 0
				if used := quota.DiskUsage(); used >= quota.MaxDataDirSize {
					return total, fmt.Errorf("%w: used=%d cap=%d",
						errDataDirQuotaExceeded, used, quota.MaxDataDirSize)
				}
			}
		}
		if rerr == io.EOF {
			return total, nil
		}
		if rerr != nil {
			return total, rerr
		}
	}
}

// CollectedOutput describes one output file ready for staging commit. The
// staging file is at TmpPath; FinalPath is the post-rename location chosen
// by Phase B.
type CollectedOutput struct {
	Role      string `json:"role"`
	StagingID string `json:"staging_id"`
	TmpPath   string `json:"tmp_path"`
	FinalPath string `json:"final_path"`
	Sha256    string `json:"sha256"`
	Size      int64  `json:"size"`
}

// CollectQuota enforces the max_data_dir_size soft cap during output
// collection. dispatch/exec_dispatch/staging PUT already gate
// new work on DiskUsage() before they start, but a long-running mission
// that started under the cap could still emit gigabytes of outputs into
// the data_dir without consulting it. CollectQuota re-consults the cached
// DiskUsage roughly every 16 MiB during io.Copy and aborts the file (and
// the whole collection) with data_dir_quota_exceeded when the cap is
// crossed. nil disables the check.
type CollectQuota struct {
	DiskUsage      func() int64
	MaxDataDirSize int64
}

// quotaCheckInterval mirrors the staging PUT mid-stream cadence.
const quotaCheckInterval = int64(16 << 20)

// CollectOutputs runs the TOCTOU-safe pipeline against
// workdir/out/<key> for each key. Either every key is collected (and the
// returned slice has len(keys) entries) or the function returns an error and
// any tmp files already created for earlier keys are removed.
//
// The pipeline opens work/out as a directory fd (O_DIRECTORY|O_NOFOLLOW),
// then openat each key with O_RDONLY|O_NOFOLLOW and fstat to verify it's a
// regular file. The data is copied from that fd (so the mission can no
// longer swap inodes) into <data_dir>/staging/<sh1>/<sh2>/<id>.tmp with
// concurrent sha256.
//
// quota, when non-nil, enforces max_data_dir_size mid-collection.
func CollectOutputs(workdir, dataDir string, keys []string, maxFileSize int64, quota *CollectQuota) ([]CollectedOutput, error) {
	outDir := filepath.Join(workdir, "out")
	dirFd, err := unix.Open(outDir, unix.O_DIRECTORY|unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("output_path_escape: open %s: %w", outDir, err)
	}
	defer func() { _ = unix.Close(dirFd) }()

	results := make([]CollectedOutput, 0, len(keys))
	cleanup := func() {
		for _, r := range results {
			_ = os.Remove(r.TmpPath)
		}
	}

	// Pre-check the quota once before opening any output. Cheap (the
	// DiskUsage callback returns a cached value) and avoids creating a
	// tmp file just to immediately delete it when we're already over.
	if quota != nil && quota.MaxDataDirSize > 0 && quota.DiskUsage != nil {
		if used := quota.DiskUsage(); used >= quota.MaxDataDirSize {
			return nil, fmt.Errorf("data_dir_quota_exceeded: used=%d cap=%d", used, quota.MaxDataDirSize)
		}
	}

	for _, key := range keys {
		co, err := collectOne(dirFd, dataDir, key, maxFileSize, quota)
		if err != nil {
			cleanup()
			return nil, err
		}
		results = append(results, co)
	}
	return results, nil
}

func collectOne(dirFd int, dataDir, key string, maxFileSize int64, quota *CollectQuota) (CollectedOutput, error) {
	var zero CollectedOutput
	fileFd, err := unix.Openat(dirFd, key, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return zero, fmt.Errorf("missing_output: %s", key)
		}
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EMLINK) {
			return zero, fmt.Errorf("output_path_escape: %s (%w)", key, err)
		}
		return zero, fmt.Errorf("openat %s: %w", key, err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fileFd, &st); err != nil {
		_ = unix.Close(fileFd)
		return zero, fmt.Errorf("fstat %s: %w", key, err)
	}
	if uint32(st.Mode)&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Close(fileFd)
		return zero, fmt.Errorf("output_not_regular_file: %s", key)
	}
	if maxFileSize > 0 && st.Size > maxFileSize {
		_ = unix.Close(fileFd)
		return zero, fmt.Errorf("output_too_large: %s (size=%d)", key, st.Size)
	}

	stagingID := ids.NewUUIDv7()
	shard, err := ids.ShardPath(stagingID)
	if err != nil {
		_ = unix.Close(fileFd)
		return zero, fmt.Errorf("shard %s: %w", stagingID, err)
	}
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		_ = unix.Close(fileFd)
		return zero, fmt.Errorf("mkdir staging: %w", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)

	f := os.NewFile(uintptr(fileFd), key)
	defer func() { _ = f.Close() }()

	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return zero, fmt.Errorf("create tmp %s: %w", tmpPath, err)
	}

	hw := sha256.New()
	mw := io.MultiWriter(out, hw)

	bytesCopied, err := copyWithQuota(mw, f, maxFileSize, quota)
	if err != nil {
		_ = out.Close()
		_ = os.Remove(tmpPath)
		// Distinct error category preserved by copyWithQuota — surface
		// to caller untouched so mapCollectErrorToReason can route.
		if errors.Is(err, errDataDirQuotaExceeded) {
			return zero, err
		}
		if errors.Is(err, errOutputTooLarge) {
			return zero, fmt.Errorf("output_too_large: %s (size > %d)", key, maxFileSize)
		}
		return zero, fmt.Errorf("copy %s: %w", key, err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmpPath)
		return zero, fmt.Errorf("fsync tmp: %w", err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return zero, fmt.Errorf("close tmp: %w", err)
	}
	if err := fsutil.SyncDir(stagingDir); err != nil {
		_ = os.Remove(tmpPath)
		return zero, fmt.Errorf("syncdir %s: %w", stagingDir, err)
	}

	return CollectedOutput{
		Role:      key,
		StagingID: stagingID,
		TmpPath:   tmpPath,
		FinalPath: finalPath,
		Sha256:    hex.EncodeToString(hw.Sum(nil)),
		Size:      bytesCopied,
	}, nil
}
