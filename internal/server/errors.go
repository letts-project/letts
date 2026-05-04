// Package server contains the HTTP server, router, and shared response helpers.
package server

import (
	"net/http"

	"letts/internal/server/httputil"
)

// ErrorResponse is the canonical error JSON shape.
// Re-exported from httputil for callers that import only this package.
type ErrorResponse = httputil.ErrorResponse

// WriteError writes an ErrorResponse with the given status code.
// Use this from every handler — never write inline JSON.
func WriteError(w http.ResponseWriter, status int, code, message string, details any) {
	httputil.WriteError(w, status, code, message, details)
}

// WriteJSON writes a successful JSON response.
func WriteJSON(w http.ResponseWriter, status int, body any) {
	httputil.WriteJSON(w, status, body)
}
