package lettsconfig

import (
	"errors"
	"testing"
)

func TestSubstituteEnvSimple(t *testing.T) {
	getenv := func(k string) (string, bool) {
		return map[string]string{"FOO": "bar"}[k], k == "FOO"
	}
	got, err := SubstituteEnv("prefix-${FOO}-suffix", getenv)
	if err != nil {
		t.Fatal(err)
	}
	if got != "prefix-bar-suffix" {
		t.Errorf("got %q, want prefix-bar-suffix", got)
	}
}

func TestSubstituteEnvMissingVar(t *testing.T) {
	getenv := func(k string) (string, bool) { return "", false }
	_, err := SubstituteEnv("${UNSET}", getenv)
	var me *MissingEnvError
	if !errors.As(err, &me) {
		t.Fatalf("expected MissingEnvError, got %v", err)
	}
	if me.Name != "UNSET" {
		t.Errorf("name = %q, want UNSET", me.Name)
	}
}

func TestSubstituteEnvNoVars(t *testing.T) {
	got, err := SubstituteEnv("plain-value", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "plain-value" {
		t.Errorf("got %q", got)
	}
}

func TestSubstituteEnvMultipleVars(t *testing.T) {
	getenv := func(k string) (string, bool) {
		v := map[string]string{"A": "1", "B": "2"}[k]
		return v, v != ""
	}
	got, err := SubstituteEnv("${A}-${B}-${A}", getenv)
	if err != nil {
		t.Fatal(err)
	}
	if got != "1-2-1" {
		t.Errorf("got %q", got)
	}
}

func TestIsPlainToken(t *testing.T) {
	if IsPlainToken("${FOO}") {
		t.Error("IsPlainToken(${FOO}) = true, want false")
	}
	if !IsPlainToken("plain-string") {
		t.Error("IsPlainToken(plain-string) = false, want true")
	}
	if !IsPlainToken("partial-${FOO}-plain") {
		t.Error("IsPlainToken(partial-${FOO}-plain) = false, want true")
	}
}
