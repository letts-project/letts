package outputfile

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

var combinedMu sync.Mutex

// appendCombined writes one NDJSON line to f:
//
//	{"t": <unix_ms>, "stream": "stdout"|"stderr", "data": "<utf8 string>"}
//
// or, for non-UTF-8 bytes:
//
//	{"t": <unix_ms>, "stream": "stdout"|"stderr", "data": "<base64>", "encoding": "base64"}
//
// combinedMu serialises writes so that lines don't interleave when called
// from concurrent goroutines.
func appendCombined(f *os.File, data []byte, isStderr bool) {
	combinedMu.Lock()
	defer combinedMu.Unlock()

	stream := "stdout"
	if isStderr {
		stream = "stderr"
	}

	if isUTF8(data) {
		type envUTF8 struct {
			T      int64  `json:"t"`
			Stream string `json:"stream"`
			Data   string `json:"data"`
		}
		env := envUTF8{
			T:      time.Now().UnixMilli(),
			Stream: stream,
			Data:   string(data),
		}
		if line, err := json.Marshal(env); err == nil {
			f.Write(append(line, '\n')) //nolint:errcheck
		}
	} else {
		// Non-UTF-8: let json.Marshal base64-encode the []byte value automatically.
		type envB64 struct {
			T        int64  `json:"t"`
			Stream   string `json:"stream"`
			Data     []byte `json:"data"`
			Encoding string `json:"encoding"`
		}
		env := envB64{
			T:        time.Now().UnixMilli(),
			Stream:   stream,
			Data:     data,
			Encoding: "base64",
		}
		if line, err := json.Marshal(env); err == nil {
			f.Write(append(line, '\n')) //nolint:errcheck
		}
	}
}

// isUTF8 returns true iff b is valid UTF-8.
func isUTF8(b []byte) bool {
	for i := 0; i < len(b); {
		_, size := decodeRune(b[i:])
		if size == 0 {
			return false
		}
		i += size
	}
	return true
}

// decodeRune is a lightweight UTF-8 decoder that returns (rune, 0) on invalid
// byte sequences.
func decodeRune(b []byte) (rune, int) {
	if len(b) == 0 {
		return 0, 0
	}
	c := b[0]
	switch {
	case c < 0x80:
		return rune(c), 1
	case c&0xE0 == 0xC0 && len(b) >= 2 && b[1]&0xC0 == 0x80:
		r := rune(c&0x1F)<<6 | rune(b[1]&0x3F)
		if r < 0x80 {
			return 0, 0 // overlong
		}
		return r, 2
	case c&0xF0 == 0xE0 && len(b) >= 3 && b[1]&0xC0 == 0x80 && b[2]&0xC0 == 0x80:
		r := rune(c&0x0F)<<12 | rune(b[1]&0x3F)<<6 | rune(b[2]&0x3F)
		if r < 0x800 {
			return 0, 0 // overlong
		}
		return r, 3
	case c&0xF8 == 0xF0 && len(b) >= 4 && b[1]&0xC0 == 0x80 && b[2]&0xC0 == 0x80 && b[3]&0xC0 == 0x80:
		r := rune(c&0x07)<<18 | rune(b[1]&0x3F)<<12 | rune(b[2]&0x3F)<<6 | rune(b[3]&0x3F)
		if r < 0x10000 {
			return 0, 0 // overlong
		}
		return r, 4
	}
	return 0, 0
}
