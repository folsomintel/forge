// forged is the whole product in one binary:
//
//	forged serve              - run the server
//	forged keygen --name X    - generate a client keypair, register the public key
//	forged addkey --pem-b64 P - register an existing client public key
//	forged token --key K ...  - mint a test JWT (customers self-sign in prod)
//	forged openapi            - print the generated OpenAPI spec
//	forged hook pre-receive   - internal: invoked by git during pushes
package main

import (
	"fmt"
	"os"
	"runtime"
)

// version is stamped at build time via -ldflags "-X main.version=...".
// goreleaser sets it from the git tag; a plain `go build` leaves "dev".
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: forged <serve|keygen|addkey|delkey|token|credential|openapi|hook|version>")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "version", "-v", "--version":
		fmt.Printf("forged %s %s/%s %s\n", version, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return
	case "serve":
		err = serve()
	case "keygen":
		err = keygen(os.Args[2:])
	case "token":
		err = token(os.Args[2:])
	case "credential":
		err = credential(os.Args[2:])
	case "addkey":
		err = addkey(os.Args[2:])
	case "delkey":
		err = delkey(os.Args[2:])
	case "openapi":
		err = printOpenAPI()
	case "hook":
		err = hook(os.Args[2:])
	case "storm":
		err = storm(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "forged:", err)
		os.Exit(1)
	}
}
