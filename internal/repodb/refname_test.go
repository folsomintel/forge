package repodb

import "testing"

func TestValidRefName(t *testing.T) {
	for name, want := range map[string]bool{
		"refs/heads/main":                        true,
		"refs/heads/feature/x-1.2":               true,
		"refs/tags/v1.0":                         true,
		"refs/namespaces/abc/refs/heads/scratch": true,
		"refs/heads/../../../escaped":            false, // path traversal
		"refs/../config":                         false,
		"refs/heads/.hidden":                     false,
		"refs/heads/x.lock":                      false,
		"refs/heads/x.lock/y":                    false,
		"refs/heads/x.":                          false,
		"refs/heads//x":                          false,
		"refs/heads/":                            false,
		"refs/":                                  false,
		"HEAD":                                   false,
		"heads/main":                             false,
		"refs/heads/a@{1}":                       false,
		"refs/heads/a b":                         false,
		"refs/heads/a\x00b":                      false,
		"refs/heads/a~1":                         false,
		"refs/heads/a\\b":                        false,
	} {
		if got := ValidRefName(name); got != want {
			t.Errorf("ValidRefName(%q) = %v, want %v", name, got, want)
		}
	}
}
