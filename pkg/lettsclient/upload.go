package lettsclient

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// UploadFile uploads localPath to stagingID via HEAD-then-PUT.
// Returns stagingID, sha256 hex, total size.
//
// Algorithm:
//  1. Open file, compute sha256 and size.
//  2. HEAD /v1/staging/{id}.
//     - 404 → PUT initial.
//     - 200 complete with same sha → skip.
//     - 200 complete with different sha → error.
//     - 200 incomplete with same sha → seek file to BytesReceived, PUT resume.
//     - 200 incomplete with different sha → error.
func UploadFile(c *Client, stagingID, localPath string) (id string, sha256hex string, size int64, err error) {
	f, err := os.Open(localPath)
	if err != nil {
		return "", "", 0, fmt.Errorf("open %s: %w", localPath, err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", "", 0, fmt.Errorf("hash %s: %w", localPath, err)
	}
	hexSum := hex.EncodeToString(h.Sum(nil))

	head, err := HeadStaging(c, stagingID)
	if err != nil {
		return "", "", 0, fmt.Errorf("head: %w", err)
	}
	switch head.Status {
	case StagingNotFound:
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return "", "", 0, err
		}
		if err := PutStagingInitial(c, stagingID, hexSum, n, f); err != nil {
			return "", "", 0, fmt.Errorf("put initial: %w", err)
		}
	case StagingComplete:
		if head.SHA256 != "" && head.SHA256 != hexSum {
			return "", "", 0, fmt.Errorf("staging %q already exists with different sha %s (local %s)", stagingID, head.SHA256, hexSum)
		}
	case StagingIncomplete:
		if head.SHA256 != "" && head.SHA256 != hexSum {
			return "", "", 0, fmt.Errorf("staging %q in progress with different sha %s (local %s)", stagingID, head.SHA256, hexSum)
		}
		if head.TotalSize != 0 && head.TotalSize != n {
			return "", "", 0, fmt.Errorf("staging %q in progress with different size %d (local %d)", stagingID, head.TotalSize, n)
		}
		if _, err := f.Seek(head.BytesReceived, io.SeekStart); err != nil {
			return "", "", 0, err
		}
		if err := PutStagingResume(c, stagingID, hexSum, n, head.BytesReceived, f); err != nil {
			return "", "", 0, fmt.Errorf("put resume: %w", err)
		}
	default:
		return "", "", 0, fmt.Errorf("unknown HEAD status %d", head.Status)
	}
	return stagingID, hexSum, n, nil
}
