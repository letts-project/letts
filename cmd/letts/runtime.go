package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// appCtx bridges cobra subcommand handlers to lettsconfig and lettsclient.
// Handlers receive a single *appCtx and pull per-host clients lazily.
//
// Caching key is (HostID, Scope) so admin and dispatch clients for the
// same host stay distinct (different tokens).
type appCtx struct {
	Config       *lettsconfig.Config
	Getenv       lettsconfig.EnvLookup
	BaseURLForID map[string]string // tests inject overrides
	Verbose      bool              // --verbose: emit debug diagnostics to Stderr
	Quiet        bool              // --quiet: suppress informational stderr
	Stderr       io.Writer         // diagnostics sink (nil → os.Stderr)

	mu      sync.Mutex
	clients map[clientKey]*hostClient
}

// errw is the diagnostics sink: the command's stderr when set (so output
// redirection and tests capture it), otherwise the process stderr.
func (a *appCtx) errw() io.Writer {
	if a.Stderr != nil {
		return a.Stderr
	}
	return os.Stderr
}

// debugf writes a debug diagnostic line when --verbose is set. Diagnostics go
// to stderr so they never pollute --output=json/yaml on stdout.
func (a *appCtx) debugf(format string, args ...any) {
	if a == nil || !a.Verbose {
		return
	}
	fmt.Fprintf(a.errw(), "letts: debug: "+format+"\n", args...)
}

type clientKey struct {
	HostID string
	Scope  lettsconfig.Scope
}

type hostClient = lettsclient.Client

type appCtxOpts struct {
	ConfigPath string
	// Insecure mirrors --insecure-config-permissions: skip the letts.yaml
	// 0600/0400 perm check when plain-text tokens are present.
	Insecure bool
	// Verbose mirrors --verbose: emit debug diagnostics to stderr.
	Verbose bool
	// Stderr is the diagnostics sink (nil → os.Stderr). setupAppCtx wires the
	// cobra command's stderr here so redirection and tests capture diagnostics.
	Stderr io.Writer
}

func newAppCtx(opts appCtxOpts) (*appCtx, error) {
	cwd, _ := os.Getwd()
	home, _ := os.UserHomeDir()
	path, err := lettsconfig.Discover(lettsconfig.DiscoverOpts{
		Flag:    opts.ConfigPath,
		Getenv:  func(k string) (string, bool) { return os.LookupEnv(k) },
		Cwd:     cwd,
		HomeDir: home,
	})
	if err != nil {
		return nil, NewConfigError(err.Error())
	}
	if opts.Verbose {
		w := opts.Stderr
		if w == nil {
			w = os.Stderr
		}
		fmt.Fprintf(w, "letts: debug: resolved config %s\n", path)
	}
	cfg, err := lettsconfig.LoadAndResolveWithOpts(path, lettsconfig.ResolveOpts{Insecure: opts.Insecure})
	if err != nil {
		return nil, NewConfigError(err.Error())
	}
	return &appCtx{
		Config:  cfg,
		Getenv:  func(k string) (string, bool) { return os.LookupEnv(k) },
		clients: map[clientKey]*hostClient{},
		Verbose: opts.Verbose,
		Stderr:  opts.Stderr,
	}, nil
}

// Close releases per-host clients. Currently a no-op; reserved for future
// teardown of connection pools.
func (a *appCtx) Close() {}

func (a *appCtx) baseURLFor(dugdaleID string) (string, error) {
	if a.BaseURLForID != nil {
		if v, ok := a.BaseURLForID[dugdaleID]; ok {
			return v, nil
		}
	}
	return lettsconfig.BaseURLFor(a.Config, dugdaleID)
}

// ClientForHost returns a cached *lettsclient.Client for (dugdaleID, scope),
// constructing one on first use. Safe for concurrent callers.
func (a *appCtx) ClientForHost(dugdaleID string, scope lettsconfig.Scope) (*lettsclient.Client, error) {
	// Resolve aliases (incl. ${VAR}-substituted values) up front so
	// ctl/events/logs/staging accept --host=<alias> like dispatch/run/exec —
	// both the URL and token lookups below key on the real id. On resolution
	// failure (e.g. a test-only id present only in BaseURLForID, not Config)
	// fall back to the raw id and let baseURLFor handle it.
	if a.Config != nil {
		if resolved, rerr := lettsconfig.ResolveHost(a.Config, dugdaleID, a.Getenv); rerr == nil {
			dugdaleID = resolved
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := clientKey{HostID: dugdaleID, Scope: scope}
	if c, ok := a.clients[key]; ok {
		return c, nil
	}
	base, err := a.baseURLFor(dugdaleID)
	if err != nil {
		return nil, err
	}
	tok, err := lettsconfig.ResolveToken(a.Config, dugdaleID, scope, a.Getenv)
	if err != nil {
		return nil, NewConfigError(err.Error())
	}
	a.debugf("host %s -> %s (scope %v)", dugdaleID, base, scope)
	c, err := lettsclient.New(lettsclient.Options{BaseURL: base, Token: tok})
	if err != nil {
		return nil, err
	}
	a.clients[key] = c
	return c, nil
}
