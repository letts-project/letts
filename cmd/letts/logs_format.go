package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// outputFrame is one chunk of mission output: a line of the daemon's
// combined NDJSON stream, or one line of a raw stdout/stderr stream (T unset).
type outputFrame struct {
	T        int64  `json:"t,omitempty" yaml:"t,omitempty"`
	Stream   string `json:"stream" yaml:"stream"`
	Data     string `json:"data" yaml:"data"`
	Encoding string `json:"encoding,omitempty" yaml:"encoding,omitempty"`
}

func (fr outputFrame) bytes() ([]byte, error) {
	if fr.Encoding == "base64" {
		return base64.StdEncoding.DecodeString(fr.Data)
	}
	return []byte(fr.Data), nil
}

// renderOutput writes an /output response body for stream in format f:
//   - text: the decoded output bytes;
//   - ndjson: one frame per line;
//   - json/yaml: a single array of frames, written once body ends.
func renderOutput(w io.Writer, body io.Reader, stream string, f Format) error {
	combined := stream == "combined"
	if (f == FormatText && !combined) || (f == FormatNDJSON && combined) {
		_, err := io.Copy(w, body)
		return err
	}

	frames := []outputFrame{}
	br := bufio.NewReader(body)
	for {
		line, rerr := br.ReadBytes('\n')
		if rerr != nil && rerr != io.EOF {
			return rerr
		}
		complete := rerr == nil
		if len(line) > 0 {
			fr, ok, err := parseOutputLine(line, stream, complete)
			if err != nil {
				if f != FormatText {
					return err
				}
				if _, werr := w.Write(line); werr != nil {
					return werr
				}
			} else if ok {
				if err := emitOutputFrame(w, fr, f, &frames); err != nil {
					return err
				}
			}
		}
		if !complete {
			break
		}
	}

	switch f {
	case FormatJSON:
		return PrintJSON(w, frames)
	case FormatYAML:
		return PrintYAML(w, frames)
	}
	return nil
}

// parseOutputLine turns one line of the body into a frame. ok=false means the
// line carries no frame and is skipped.
func parseOutputLine(line []byte, stream string, complete bool) (outputFrame, bool, error) {
	if stream != "combined" {
		if utf8.Valid(line) {
			return outputFrame{Stream: stream, Data: string(line)}, true, nil
		}
		return outputFrame{Stream: stream, Data: base64.StdEncoding.EncodeToString(line), Encoding: "base64"}, true, nil
	}
	var fr outputFrame
	if err := json.Unmarshal(line, &fr); err != nil {
		// The daemon writes every combined frame newline-terminated in a single
		// write, so an unterminated tail is a frame still being written when a
		// non-follow read hit EOF.
		if !complete {
			return outputFrame{}, false, nil
		}
		return outputFrame{}, false, fmt.Errorf("malformed combined output frame %q: %w", bytes.TrimSpace(line), err)
	}
	return fr, true, nil
}

func emitOutputFrame(w io.Writer, fr outputFrame, f Format, frames *[]outputFrame) error {
	switch f {
	case FormatText:
		b, err := fr.bytes()
		if err != nil {
			return fmt.Errorf("decode %s output frame: %w", fr.Stream, err)
		}
		_, err = w.Write(b)
		return err
	case FormatNDJSON:
		return PrintNDJSON(w, fr)
	default:
		*frames = append(*frames, fr)
		return nil
	}
}
