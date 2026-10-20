package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const combinedBody = `{"t":1,"stream":"stdout","data":"book 1027: loss -> re-merged\n"}
{"t":2,"stream":"stderr","data":"warn: slow\n"}
{"t":3,"stream":"stdout","data":"/w==","encoding":"base64"}
{"t":4,"stream":"stdout","data":"tail"}
`

func render(t *testing.T, body, stream string, f Format) string {
	t.Helper()
	var out bytes.Buffer
	if err := renderOutput(&out, strings.NewReader(body), stream, f); err != nil {
		t.Fatalf("renderOutput(%s, %v): %v", stream, f, err)
	}
	return out.String()
}

func TestRenderOutputCombinedText(t *testing.T) {
	got := render(t, combinedBody, "combined", FormatText)
	want := "book 1027: loss -> re-merged\nwarn: slow\n\xfftail"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderOutputCombinedNDJSONIsVerbatim(t *testing.T) {
	if got := render(t, combinedBody, "combined", FormatNDJSON); got != combinedBody {
		t.Errorf("got %q, want body verbatim", got)
	}
}

func TestRenderOutputCombinedJSON(t *testing.T) {
	got := render(t, combinedBody, "combined", FormatJSON)
	var frames []outputFrame
	if err := json.Unmarshal([]byte(got), &frames); err != nil {
		t.Fatalf("not a JSON array: %v\n%s", err, got)
	}
	want := []outputFrame{
		{T: 1, Stream: "stdout", Data: "book 1027: loss -> re-merged\n"},
		{T: 2, Stream: "stderr", Data: "warn: slow\n"},
		{T: 3, Stream: "stdout", Data: "/w==", Encoding: "base64"},
		{T: 4, Stream: "stdout", Data: "tail"},
	}
	if len(frames) != len(want) {
		t.Fatalf("got %d frames, want %d: %+v", len(frames), len(want), frames)
	}
	for i := range want {
		if frames[i] != want[i] {
			t.Errorf("frame %d = %+v, want %+v", i, frames[i], want[i])
		}
	}
	if strings.Count(got, "\n") < len(want) {
		t.Errorf("json output not indented: %q", got)
	}
}

func TestRenderOutputCombinedYAML(t *testing.T) {
	got := render(t, combinedBody, "combined", FormatYAML)
	var frames []outputFrame
	if err := yaml.Unmarshal([]byte(got), &frames); err != nil {
		t.Fatalf("not YAML: %v\n%s", err, got)
	}
	if len(frames) != 4 || frames[1].Stream != "stderr" || frames[1].Data != "warn: slow\n" || frames[2].Encoding != "base64" {
		t.Errorf("unexpected frames: %+v", frames)
	}
	if strings.HasPrefix(strings.TrimSpace(got), "{") || strings.HasPrefix(strings.TrimSpace(got), "[") {
		t.Errorf("yaml output looks like JSON: %q", got)
	}
}

func TestRenderOutputEmptyBody(t *testing.T) {
	for _, stream := range []string{"combined", "stdout"} {
		if got := render(t, "", stream, FormatJSON); strings.TrimSpace(got) != "[]" {
			t.Errorf("%s json: got %q, want []", stream, got)
		}
		if got := render(t, "", stream, FormatYAML); strings.TrimSpace(got) != "[]" {
			t.Errorf("%s yaml: got %q, want []", stream, got)
		}
		for _, f := range []Format{FormatText, FormatNDJSON} {
			if got := render(t, "", stream, f); got != "" {
				t.Errorf("%s %v: got %q, want empty", stream, f, got)
			}
		}
	}
}

const rawBody = "line one\n\xff\xfe\nlast"

func TestRenderOutputRawText(t *testing.T) {
	if got := render(t, rawBody, "stderr", FormatText); got != rawBody {
		t.Errorf("got %q, want body verbatim", got)
	}
}

func TestRenderOutputRawNDJSON(t *testing.T) {
	got := render(t, rawBody, "stderr", FormatNDJSON)
	lines := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
	want := []outputFrame{
		{Stream: "stderr", Data: "line one\n"},
		{Stream: "stderr", Data: "//4K", Encoding: "base64"},
		{Stream: "stderr", Data: "last"},
	}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d: %q", len(lines), len(want), got)
	}
	for i, line := range lines {
		var fr outputFrame
		if err := json.Unmarshal([]byte(line), &fr); err != nil {
			t.Fatalf("line %d not JSON: %q", i, line)
		}
		if fr != want[i] {
			t.Errorf("line %d = %+v, want %+v", i, fr, want[i])
		}
		if strings.Contains(line, `"t"`) {
			t.Errorf("raw-stream frame must not carry t: %q", line)
		}
	}
}

func TestRenderOutputRawJSONRoundTripsBytes(t *testing.T) {
	got := render(t, rawBody, "stdout", FormatJSON)
	var frames []outputFrame
	if err := json.Unmarshal([]byte(got), &frames); err != nil {
		t.Fatalf("not a JSON array: %v\n%s", err, got)
	}
	var joined []byte
	for _, fr := range frames {
		if fr.Stream != "stdout" {
			t.Errorf("stream=%q want stdout", fr.Stream)
		}
		b, err := fr.bytes()
		if err != nil {
			t.Fatal(err)
		}
		joined = append(joined, b...)
	}
	if string(joined) != rawBody {
		t.Errorf("decoded frames = %q, want %q", joined, rawBody)
	}
}

func TestRenderOutputCombinedSkipsUnterminatedPartialFrame(t *testing.T) {
	body := `{"t":1,"stream":"stdout","data":"a\n"}` + "\n" + `{"t":2,"stream":"std`
	if got := render(t, body, "combined", FormatText); got != "a\n" {
		t.Errorf("text: got %q, want %q", got, "a\n")
	}
	var frames []outputFrame
	if err := json.Unmarshal([]byte(render(t, body, "combined", FormatJSON)), &frames); err != nil || len(frames) != 1 {
		t.Errorf("json: frames=%+v err=%v, want 1 frame", frames, err)
	}
}

func TestRenderOutputCombinedMalformedLine(t *testing.T) {
	body := `{"t":1,"stream":"stdout","data":"a\n"}` + "\nnot json\n" + `{"t":2,"stream":"stdout","data":"b\n"}` + "\n"
	if got := render(t, body, "combined", FormatText); got != "a\nnot json\nb\n" {
		t.Errorf("text: got %q, want malformed line passed through", got)
	}
	for _, f := range []Format{FormatJSON, FormatYAML} {
		var out bytes.Buffer
		err := renderOutput(&out, strings.NewReader(body), "combined", f)
		if err == nil || !strings.Contains(err.Error(), "malformed combined output frame") {
			t.Errorf("%v: err=%v, want malformed-frame error", f, err)
		}
		if out.Len() != 0 {
			t.Errorf("%v: wrote %q before failing", f, out.String())
		}
	}
}
