package lettsclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"
)

// Event is one decoded line from /v1/missions/{id}/events.
//
// Field shape mirrors daemon emission (see internal/mission/finalize.go,
// internal/mission/waiter.go, internal/mission/fd3writer.go,
// internal/server/handlers/dispatch.go):
//
//   - queued:   mission_id, time_created, lane, mission
//   - running:  time, pid, time_started (first run only; relane re-emits only time)
//   - progress: time, optional value, optional message
//   - done:     time_finished, outcome, exit_code, optional signal/return/fail_*,
//     optional outputs (map keyed by role), optional duration_ms
//
// The done event carries time_finished (not "time") and an outputs map
// with staging_id/sha256/size so clients can download outputs without a
// follow-up GET /v1/missions/{id}.
type Event struct {
	Seq          int64                  `json:"seq"`
	Event        string                 `json:"event"`
	MissionID    string                 `json:"mission_id,omitempty"`
	Lane         string                 `json:"lane,omitempty"`
	Mission      string                 `json:"mission,omitempty"`
	Time         int64                  `json:"time,omitempty"`
	TimeCreated  int64                  `json:"time_created,omitempty"`
	TimeStarted  int64                  `json:"time_started,omitempty"`
	TimeFinished int64                  `json:"time_finished,omitempty"`
	Pid          int                    `json:"pid,omitempty"`
	Value        float64                `json:"value,omitempty"`
	Message      string                 `json:"message,omitempty"`
	Outcome      string                 `json:"outcome,omitempty"`
	ExitCode     *int                   `json:"exit_code,omitempty"`
	Signal       string                 `json:"signal,omitempty"`
	FailReason   string                 `json:"fail_reason,omitempty"`
	FailMessage  string                 `json:"fail_message,omitempty"`
	FailDetails  json.RawMessage        `json:"fail_details,omitempty"`
	Return       json.RawMessage        `json:"return,omitempty"`
	Outputs      map[string]EventOutput `json:"outputs,omitempty"`
	DurationMs   int64                  `json:"duration_ms,omitempty"`
	Raw          json.RawMessage        `json:"-"`
}

// EventOutput mirrors one entry of the done event's outputs map as emitted
// by the daemon (internal/mission/finalize.go summarizeOutputs). The map
// key is the role; each value carries staging_id/sha256/size.
type EventOutput struct {
	StagingID string `json:"staging_id"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
}

// StreamOpts configures StreamEvents.
type StreamOpts struct {
	Follow           bool
	From             int64
	ReconnectBackoff time.Duration
}

// StreamEvents opens the events stream and invokes fn for each event in
// order. If Follow is true and the stream closes without a terminal done
// event, StreamEvents reconnects with from=<last seq>.
func StreamEvents(ctx context.Context, c *Client, missionID string, opts StreamOpts, fn func(Event) error) error {
	if opts.ReconnectBackoff <= 0 {
		opts.ReconnectBackoff = 1 * time.Second
	}
	lastSeq := opts.From
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		terminal, lastSeen, err := streamOnce(ctx, c, missionID, opts.Follow, lastSeq, fn)
		if lastSeen > lastSeq {
			lastSeq = lastSeen
		}
		if terminal {
			return nil
		}
		if err != nil {
			// 404 (mission GC'd) / 410 (deleting) are terminal — the
			// mission is gone, not a transient disconnect. Return instead of
			// reconnecting once per ReconnectBackoff forever (which, combined
			// with run's default-infinite wait, would spin until Ctrl-C).
			var he *HTTPError
			if errors.As(err, &he) && (he.Status == 404 || he.Status == 410) {
				return err
			}
			if !opts.Follow {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(opts.ReconnectBackoff):
			}
			continue
		}
		if !opts.Follow {
			return fmt.Errorf("event stream ended without terminal done event")
		}
	}
}

func streamOnce(ctx context.Context, c *Client, missionID string, follow bool, from int64, fn func(Event) error) (terminal bool, lastSeq int64, err error) {
	q := url.Values{}
	if follow {
		q.Set("follow", "true")
	}
	if from > 0 {
		q.Set("from", strconv.FormatInt(from, 10))
	}
	path := "/v1/missions/" + url.PathEscape(missionID) + "/events"
	if e := q.Encode(); e != "" {
		path += "?" + e
	}
	req, err := c.newRequest("GET", path, nil, nil)
	if err != nil {
		return false, 0, err
	}
	req = req.WithContext(ctx)
	resp, err := c.hc.Do(req)
	if err != nil {
		return false, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return false, 0, decodeHTTPError(resp, req.URL.String())
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return false, lastSeq, fmt.Errorf("decode event: %w", err)
		}
		ev.Raw = append([]byte(nil), line...)
		if err := fn(ev); err != nil {
			return false, lastSeq, err
		}
		if ev.Seq > lastSeq {
			lastSeq = ev.Seq
		}
		if ev.Event == "done" {
			return true, lastSeq, nil
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return false, lastSeq, err
	}
	return false, lastSeq, nil
}
