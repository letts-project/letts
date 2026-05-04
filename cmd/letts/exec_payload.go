package main

import "letts/pkg/lettsclient"

// execPayloadInputs is the input bag for buildExecRequest. Decoupling from
// execFlags lets fan-out subtly override (mission_id, group_id) per host
// without copying the whole flag struct.
type execPayloadInputs struct {
	missionID  string
	lane       string
	argv       []string
	timeout    string
	hostsCount int
	groupID    string

	// Content delivery: script/in/out/stdin
	scriptStagingID string
	scriptPath      string
	in              []lettsclient.ExecFileRef
	out             []string
	stdinMode       string
	stdinStagingID  string
}

// buildExecRequest is a pure function that assembles a server-bound
// ExecRequest. Display_name is computed via buildDisplayName. Caller must
// have validated key syntax for --in/--out via parseKVList.
func buildExecRequest(p execPayloadInputs) lettsclient.ExecRequest {
	req := lettsclient.ExecRequest{
		MissionID:   p.missionID,
		Lane:        p.lane,
		Command:     p.argv,
		Timeout:     p.timeout,
		GroupID:     p.groupID,
		DisplayName: buildDisplayName(p.argv, p.scriptPath, p.hostsCount),
	}
	if p.scriptStagingID != "" {
		req.Script = &lettsclient.ExecScriptRef{StagingID: p.scriptStagingID}
	}
	if len(p.in) > 0 {
		req.In = p.in
	}
	if len(p.out) > 0 {
		req.Out = make([]lettsclient.ExecOutKey, len(p.out))
		for i, k := range p.out {
			req.Out[i] = lettsclient.ExecOutKey{Key: k}
		}
	}
	if p.stdinMode != "" && p.stdinMode != "none" {
		req.Stdin = p.stdinMode
		req.StdinStagingID = p.stdinStagingID
	}
	return req
}
