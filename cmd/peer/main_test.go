package main

import (
	"strings"
	"testing"
)

func TestRunRejectsUnknownCommand(t *testing.T) {
	err := run([]string{"unknown"})
	if err == nil || !strings.Contains(err.Error(), "Usage: peer") {
		t.Fatalf("run() error = %v, want usage guidance", err)
	}
}

func TestRunRequiresCommand(t *testing.T) {
	err := run(nil)
	if err == nil || !strings.Contains(err.Error(), "Usage: peer") {
		t.Fatalf("run() error = %v, want usage guidance", err)
	}
}
