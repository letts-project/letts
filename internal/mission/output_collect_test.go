package mission

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupOutDir(t *testing.T, files map[string]string) (workdir, dataDir string) {
	t.Helper()
	workdir = t.TempDir()
	dataDir = t.TempDir()
	out := filepath.Join(workdir, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("mkdir out: %v", err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(out, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return
}

func TestCollectOutputsHappyPath(t *testing.T) {
	workdir, dataDir := setupOutDir(t, map[string]string{"result": "hello"})
	got, err := CollectOutputs(workdir, dataDir, []string{"result"}, 0, nil)
	if err != nil {
		t.Fatalf("CollectOutputs: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d, want 1", len(got))
	}
	if got[0].Role != "result" {
		t.Errorf("Role=%q, want result", got[0].Role)
	}
	if got[0].Size != 5 {
		t.Errorf("Size=%d, want 5", got[0].Size)
	}
	expected := sha256.Sum256([]byte("hello"))
	if got[0].Sha256 != hex.EncodeToString(expected[:]) {
		t.Errorf("Sha256=%q", got[0].Sha256)
	}
	b, err := os.ReadFile(got[0].TmpPath)
	if err != nil {
		t.Fatalf("read tmp: %v", err)
	}
	if string(b) != "hello" {
		t.Errorf("tmp content=%q, want hello", string(b))
	}
	// Final path not yet created (Phase B does the rename).
	if _, err := os.Stat(got[0].FinalPath); !os.IsNotExist(err) {
		t.Errorf("final exists prematurely at %s (err=%v)", got[0].FinalPath, err)
	}
}

func TestCollectOutputsMissing(t *testing.T) {
	workdir, dataDir := setupOutDir(t, nil)
	_, err := CollectOutputs(workdir, dataDir, []string{"nope"}, 0, nil)
	if err == nil || !strings.Contains(err.Error(), "missing_output") {
		t.Errorf("err=%v, want missing_output", err)
	}
}

func TestCollectOutputsSymlinkAtKey(t *testing.T) {
	workdir, dataDir := setupOutDir(t, map[string]string{"target": "data"})
	out := filepath.Join(workdir, "out")
	if err := os.Symlink("target", filepath.Join(out, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	_, err := CollectOutputs(workdir, dataDir, []string{"link"}, 0, nil)
	if err == nil || !strings.Contains(err.Error(), "output_path_escape") {
		t.Errorf("err=%v, want output_path_escape", err)
	}
}

func TestCollectOutputsOutDirIsSymlink(t *testing.T) {
	workdir := t.TempDir()
	dataDir := t.TempDir()
	real := t.TempDir()
	if err := os.WriteFile(filepath.Join(real, "result"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink(real, filepath.Join(workdir, "out")); err != nil {
		t.Fatalf("symlink out: %v", err)
	}
	_, err := CollectOutputs(workdir, dataDir, []string{"result"}, 0, nil)
	if err == nil || !strings.Contains(err.Error(), "output_path_escape") {
		t.Errorf("err=%v, want output_path_escape", err)
	}
}

func TestCollectOutputsTooLargeByFstat(t *testing.T) {
	workdir, dataDir := setupOutDir(t, map[string]string{"big": strings.Repeat("x", 1024)})
	_, err := CollectOutputs(workdir, dataDir, []string{"big"}, 100, nil)
	if err == nil || !strings.Contains(err.Error(), "output_too_large") {
		t.Errorf("err=%v, want output_too_large", err)
	}
}

func TestCollectOutputsNotRegularFile(t *testing.T) {
	workdir, dataDir := setupOutDir(t, nil)
	out := filepath.Join(workdir, "out")
	if err := os.MkdirAll(filepath.Join(out, "subdir"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_, err := CollectOutputs(workdir, dataDir, []string{"subdir"}, 0, nil)
	if err == nil || !strings.Contains(err.Error(), "output_not_regular_file") {
		t.Errorf("err=%v, want output_not_regular_file", err)
	}
}

func TestCollectOutputsRollbackOnLaterFailure(t *testing.T) {
	workdir, dataDir := setupOutDir(t, map[string]string{"first": "a"})
	got, err := CollectOutputs(workdir, dataDir, []string{"first", "second"}, 0, nil)
	if err == nil {
		t.Fatal("expected error for missing second")
	}
	if got != nil {
		t.Errorf("results=%v, want nil on error", got)
	}
	var found []string
	_ = filepath.Walk(filepath.Join(dataDir, "staging"), func(path string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() && strings.HasSuffix(path, ".tmp") {
			found = append(found, path)
		}
		return nil
	})
	if len(found) > 0 {
		t.Errorf("orphan tmps after rollback: %v", found)
	}
}

func TestCollectOutputsMultipleKeysSuccess(t *testing.T) {
	workdir, dataDir := setupOutDir(t, map[string]string{
		"a": "alpha",
		"b": "bravo!",
	})
	got, err := CollectOutputs(workdir, dataDir, []string{"a", "b"}, 0, nil)
	if err != nil {
		t.Fatalf("CollectOutputs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	roles := map[string]CollectedOutput{}
	for _, c := range got {
		roles[c.Role] = c
	}
	if roles["a"].Size != 5 || roles["b"].Size != 6 {
		t.Errorf("sizes wrong: %+v", got)
	}
}

func TestCollectOutputsEmptyKeysReturnsEmpty(t *testing.T) {
	workdir, dataDir := setupOutDir(t, nil)
	got, err := CollectOutputs(workdir, dataDir, nil, 0, nil)
	if err != nil {
		t.Fatalf("CollectOutputs: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len=%d, want 0", len(got))
	}
}

// TestCollectOutputsQuotaPreCheckBlocks: when DiskUsage already
// reports usage above MaxDataDirSize before copy begins, abort with
// data_dir_quota_exceeded so we don't push further over the soft cap.
func TestCollectOutputsQuotaPreCheckBlocks(t *testing.T) {
	workdir, dataDir := setupOutDir(t, map[string]string{"out": "payload"})
	q := &CollectQuota{
		DiskUsage:      func() int64 { return 2048 }, // already over
		MaxDataDirSize: 1024,
	}
	_, err := CollectOutputs(workdir, dataDir, []string{"out"}, 0, q)
	if err == nil || !strings.Contains(err.Error(), "data_dir_quota_exceeded") {
		t.Errorf("err=%v, want data_dir_quota_exceeded", err)
	}
	// Tmp must have been cleaned (don't leak under quota).
	var leaks int
	_ = filepath.Walk(filepath.Join(dataDir, "staging"), func(path string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() && strings.HasSuffix(path, ".tmp") {
			leaks++
		}
		return nil
	})
	if leaks > 0 {
		t.Errorf("orphan tmp leaked: %d", leaks)
	}
}

// TestCollectOutputsQuotaTrippedDuringCopy: the soft cap is also
// enforced mid-copy. We point DiskUsage at a counter that returns
// "over" only after some bytes have been written so the in-loop branch
// is exercised, not just the pre-check.
func TestCollectOutputsQuotaTrippedDuringCopy(t *testing.T) {
	// File larger than the in-copy check interval so the periodic check
	// fires at least once. Constants intentionally match the runtime
	// 16-MiB interval used in production.
	const interval = 16 << 20
	payload := strings.Repeat("x", interval+1024)
	workdir, dataDir := setupOutDir(t, map[string]string{"big": payload})

	var used int64 = 0
	q := &CollectQuota{
		DiskUsage:      func() int64 { v := used; used = int64(2 << 30); return v },
		MaxDataDirSize: 1 << 30,
	}
	_, err := CollectOutputs(workdir, dataDir, []string{"big"}, 0, q)
	if err == nil || !strings.Contains(err.Error(), "data_dir_quota_exceeded") {
		t.Errorf("err=%v, want data_dir_quota_exceeded", err)
	}
	var leaks int
	_ = filepath.Walk(filepath.Join(dataDir, "staging"), func(path string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() && strings.HasSuffix(path, ".tmp") {
			leaks++
		}
		return nil
	})
	if leaks > 0 {
		t.Errorf("orphan tmp leaked: %d", leaks)
	}
}

// TestCollectOutputsQuotaNoOpWhenUnderCap: nil and under-cap callback both
// behave as no-op (regression guard for the no-cap default).
func TestCollectOutputsQuotaNoOpWhenUnderCap(t *testing.T) {
	workdir, dataDir := setupOutDir(t, map[string]string{"out": "ok"})
	q := &CollectQuota{
		DiskUsage:      func() int64 { return 100 },
		MaxDataDirSize: 10000,
	}
	got, err := CollectOutputs(workdir, dataDir, []string{"out"}, 0, q)
	if err != nil {
		t.Fatalf("CollectOutputs: %v", err)
	}
	if len(got) != 1 || got[0].Size != 2 {
		t.Errorf("got=%+v", got)
	}
}

func TestCollectOutputsOutDirMissing(t *testing.T) {
	workdir := t.TempDir()
	dataDir := t.TempDir()
	_, err := CollectOutputs(workdir, dataDir, []string{"x"}, 0, nil)
	if err == nil || !strings.Contains(err.Error(), "output_path_escape") {
		t.Errorf("err=%v, want output_path_escape", err)
	}
}
