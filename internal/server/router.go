package server

import (
	"net/http"

	"letts/internal/server/handlers"
)

// registerRoutes wires all HTTP routes into mux. Currently just health;
// other handlers are mounted by the caller (cmd/dugdale/main.go) after the
// full dependency graph is built.
func registerRoutes(mux *http.ServeMux, d Deps) {
	(&handlers.Health{DB: d.DB}).Register(mux)
}
