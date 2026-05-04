package main

import (
	"reflect"
	"testing"

	"letts/pkg/lettsclient"
)

func TestBuildExecRequestArgvOnly(t *testing.T) {
	got := buildExecRequest(execPayloadInputs{
		missionID:  "0192aaaa-0000-7000-8000-000000000001",
		lane:       "light",
		argv:       []string{"uptime"},
		hostsCount: 1,
	})
	want := lettsclient.ExecRequest{
		MissionID:   "0192aaaa-0000-7000-8000-000000000001",
		Lane:        "light",
		Command:     []string{"uptime"},
		DisplayName: "uptime",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}

func TestBuildExecRequestWithTimeout(t *testing.T) {
	got := buildExecRequest(execPayloadInputs{
		missionID: "0192aaaa-0000-7000-8000-000000000002",
		lane:      "light", argv: []string{"sleep", "3"},
		hostsCount: 1, timeout: "5m",
	})
	if got.Timeout != "5m" {
		t.Errorf("timeout=%q, want 5m", got.Timeout)
	}
}

func TestBuildExecRequestWithGroupID(t *testing.T) {
	got := buildExecRequest(execPayloadInputs{
		missionID: "0192aaaa-0000-7000-8000-000000000003",
		lane:      "light", argv: []string{"uptime"},
		hostsCount: 3, groupID: "0192bbbb-0000-7000-8000-000000000000",
	})
	if got.GroupID != "0192bbbb-0000-7000-8000-000000000000" {
		t.Errorf("group_id=%q", got.GroupID)
	}
	// hostsCount=3 → display_name has [+2 hosts] suffix
	if got.DisplayName != "uptime [+2 hosts]" {
		t.Errorf("display_name=%q", got.DisplayName)
	}
}

func TestBuildExecRequestWithScriptRef(t *testing.T) {
	got := buildExecRequest(execPayloadInputs{
		missionID: "0192aaaa-0000-7000-8000-000000000004",
		lane:      "light", argv: []string{"bash", "$LETTS_SCRIPT"},
		hostsCount:      1,
		scriptStagingID: "0192cccc-0000-7000-8000-000000000000",
		scriptPath:      "/tmp/convert.sh",
	})
	if got.Script == nil || got.Script.StagingID != "0192cccc-0000-7000-8000-000000000000" {
		t.Errorf("script=%+v", got.Script)
	}
	// $LETTS_SCRIPT is shell-quoted by buildDisplayName because '$' is a shell metachar.
	if got.DisplayName != `bash '$LETTS_SCRIPT' (script=convert.sh)` {
		t.Errorf("display_name=%q", got.DisplayName)
	}
}

func TestBuildExecRequestWithInOut(t *testing.T) {
	got := buildExecRequest(execPayloadInputs{
		missionID: "0192aaaa-0000-7000-8000-000000000005",
		lane:      "light", argv: []string{"x"}, hostsCount: 1,
		in: []lettsclient.ExecFileRef{
			{Key: "pdf", StagingID: "0192dddd-0000-7000-8000-000000000001"},
			{Key: "txt", StagingID: "0192dddd-0000-7000-8000-000000000002"},
		},
		out: []string{"png", "html"},
	})
	if len(got.In) != 2 || got.In[1].Key != "txt" {
		t.Errorf("in=%+v", got.In)
	}
	if len(got.Out) != 2 || got.Out[0].Key != "png" {
		t.Errorf("out=%+v", got.Out)
	}
}

func TestBuildExecRequestWithStdin(t *testing.T) {
	got := buildExecRequest(execPayloadInputs{
		missionID: "0192aaaa-0000-7000-8000-000000000006",
		lane:      "light", argv: []string{"cat"}, hostsCount: 1,
		stdinMode:      "single",
		stdinStagingID: "0192eeee-0000-7000-8000-000000000000",
	})
	if got.Stdin != "single" {
		t.Errorf("stdin=%q", got.Stdin)
	}
	if got.StdinStagingID != "0192eeee-0000-7000-8000-000000000000" {
		t.Errorf("stdin_staging_id=%q", got.StdinStagingID)
	}
}
