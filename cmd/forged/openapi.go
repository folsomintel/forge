package main

import (
	"net/http"
	"os"

	"github.com/folsomintel/forge/internal/api"
)

// printOpenAPI emits the generated spec: registration is pure (handlers
// never run), so no stores are needed.
func printOpenAPI() error {
	spec := (&api.Server{}).Register(http.NewServeMux())
	out, err := spec.YAML()
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(out)
	return err
}
