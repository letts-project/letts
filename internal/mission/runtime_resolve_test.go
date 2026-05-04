package mission

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"letts/internal/storage"
)

func TestResolveCommandDefault(t *testing.T) {
	dir := t.TempDir()
	// Create the mission file.
	missionFile := filepath.Join(dir, "my_mission")
	if err := os.WriteFile(missionFile, []byte("#!/bin/sh"), 0o755); err != nil {
		t.Fatal(err)
	}

	rt := &storage.MissionRuntime{
		MissionDir:          dir,
		MissionPathTemplate: "",
		CommandTemplate:     "",
		ValidateMissionFile: true,
	}

	argv, err := ResolveCommand(rt, "my_mission")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(argv) != 1 {
		t.Fatalf("want 1 arg, got %d: %v", len(argv), argv)
	}
	want := filepath.Join(dir, "my_mission")
	if argv[0] != want {
		t.Errorf("argv[0]: got %q, want %q", argv[0], want)
	}
}

func TestResolveCommandPHPRunner(t *testing.T) {
	dir := t.TempDir()
	// Create the mission PHP file.
	if err := os.WriteFile(filepath.Join(dir, "MyMission.php"), []byte("<?php"), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := "/path/to/runner.php"

	rt := &storage.MissionRuntime{
		MissionDir:          dir,
		MissionPathTemplate: "{mission}.php",
		CommandTemplate:     `["php", "` + runner + `", "{mission}"]`,
		ValidateMissionFile: true,
	}

	argv, err := ResolveCommand(rt, "MyMission")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(argv) != 3 {
		t.Fatalf("want 3 args, got %d: %v", len(argv), argv)
	}
	if argv[0] != "php" {
		t.Errorf("argv[0]: got %q, want %q", argv[0], "php")
	}
	if argv[1] != runner {
		t.Errorf("argv[1]: got %q, want %q", argv[1], runner)
	}
	if argv[2] != "MyMission" {
		t.Errorf("argv[2]: got %q, want %q", argv[2], "MyMission")
	}
}

func TestResolveCommandPHPSelfScript(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "MyMission.php"), []byte("<?php"), 0o644); err != nil {
		t.Fatal(err)
	}

	rt := &storage.MissionRuntime{
		MissionDir:          dir,
		MissionPathTemplate: "{mission}.php",
		CommandTemplate:     `["php", "{mission_path}"]`,
		ValidateMissionFile: true,
	}

	argv, err := ResolveCommand(rt, "MyMission")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(argv) != 2 {
		t.Fatalf("want 2 args, got %d: %v", len(argv), argv)
	}
	if argv[0] != "php" {
		t.Errorf("argv[0]: got %q", argv[0])
	}
	wantPath := filepath.Join(dir, "MyMission.php")
	if argv[1] != wantPath {
		t.Errorf("argv[1]: got %q, want %q", argv[1], wantPath)
	}
}

func TestResolveCommandMissingFile(t *testing.T) {
	dir := t.TempDir()

	rt := &storage.MissionRuntime{
		MissionDir:          dir,
		MissionPathTemplate: "",
		CommandTemplate:     "",
		ValidateMissionFile: true,
	}

	_, err := ResolveCommand(rt, "nonexistent_mission")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrMissionNotFound) {
		t.Errorf("want ErrMissionNotFound, got %v", err)
	}
}

func TestResolveCommandSymlinkEscape(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not supported on windows")
	}

	dir := t.TempDir()
	outside := t.TempDir()

	// Create a real file outside mission dir.
	outsideFile := filepath.Join(outside, "secret")
	if err := os.WriteFile(outsideFile, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create symlink inside dir pointing outside.
	link := filepath.Join(dir, "escape_mission")
	if err := os.Symlink(outsideFile, link); err != nil {
		t.Skip("symlink creation not supported: " + err.Error())
	}

	rt := &storage.MissionRuntime{
		MissionDir:          dir,
		MissionPathTemplate: "",
		CommandTemplate:     "",
		ValidateMissionFile: true,
	}

	_, err := ResolveCommand(rt, "escape_mission")
	if err == nil {
		t.Fatal("expected error for symlink escape, got nil")
	}
	if !errors.Is(err, ErrMissionNotInDir) {
		t.Errorf("want ErrMissionNotInDir, got %v", err)
	}
}
