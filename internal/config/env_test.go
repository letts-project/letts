package config

import (
	"os"
	"testing"
)

func TestExpandEnvSimple(t *testing.T) {
	t.Setenv("LETTS_TEST_TOK", "secret")
	got, err := ExpandEnv("${LETTS_TEST_TOK}", os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	if got != "secret" {
		t.Errorf("got %q", got)
	}
}

func TestExpandEnvMissing(t *testing.T) {
	if _, err := ExpandEnv("${NOT_SET_XYZ}", os.LookupEnv); err == nil {
		t.Error("expected error for missing var")
	}
}

func TestExpandEnvLiteral(t *testing.T) {
	got, err := ExpandEnv("plain", os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	if got != "plain" {
		t.Errorf("got %q", got)
	}
}

func TestExpandEnvNoExpandWithoutBraces(t *testing.T) {
	t.Setenv("LETTS_TEST_TOK", "secret")
	got, _ := ExpandEnv("$LETTS_TEST_TOK", os.LookupEnv)
	if got != "$LETTS_TEST_TOK" {
		t.Errorf("only ${VAR} should expand; got %q", got)
	}
}
