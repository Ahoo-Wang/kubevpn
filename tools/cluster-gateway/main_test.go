//go:build darwin

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestGatewayCLIRejectsInvalidInputBeforeLoadingCredentials(t *testing.T) {
	for _, args := range [][]string{{}, {"--services", "invalid"}, {"--services", "dev/app", "--refresh", "0s"}, {"--apply", "--cleanup"}, {"--apply", "--services", "dev/app"}, {"--unknown"}, {"--services", "dev/app", "extra"}} {
		var out bytes.Buffer
		if err := run(context.Background(), args, &out); err == nil {
			t.Fatalf("accepted invalid command %v", args)
		}
	}
}
func TestGatewayCLIHelpDoesNotLoadClusterOrMutateSystem(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "apply") {
		t.Fatal("activation is not explicit in help")
	}
}
func TestGatewayCLIServiceReferences(t *testing.T) {
	refs, err := parseServices("dev/app, prod/db")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0].Namespace != "dev" || refs[1].Name != "db" {
		t.Fatalf("unexpected refs %v", refs)
	}
	for _, value := range []string{"", ",", "dev/", "/app", "dev/app/extra"} {
		if _, err := parseServices(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}
