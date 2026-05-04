package server

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"

	"letts/internal/config"
)

// Deps holds everything a Server needs. Populated by main; tests construct directly.
type Deps struct {
	Cfg            *config.DugdaleConfig
	DB             *sql.DB
	Listener       net.Listener
	Logger         *slog.Logger
	TrustedProxies []*net.IPNet
}

// Server bundles the net/http server with letts-specific middleware and handlers.
type Server struct {
	deps Deps
	http *http.Server
}

// NewServer builds the router and wraps it in an http.Server.
func NewServer(d Deps) *Server {
	mux := http.NewServeMux()
	registerRoutes(mux, d)
	return &Server{
		deps: d,
		http: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10_000_000_000, // 10s in nanoseconds
		},
	}
}

// Serve starts accepting connections on d.Listener; blocks until ctx is
// canceled, then performs a 30-second graceful shutdown.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30_000_000_000)
		defer cancel()
		_ = s.http.Shutdown(shutdownCtx)
	}()
	err := s.http.Serve(s.deps.Listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
