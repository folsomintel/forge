package repodb

import "strings"

// Ephemeral branches are ordinary refs under a git ref namespace
// (gitnamespaces(7)). Serving with GIT_NAMESPACE=eph makes them appear as
// refs/heads/* to that client and invisible to everyone else; in the DB
// they live under the full namespaced name. Same object store either way -
// promoting an ephemeral branch is a ref CAS, no object copying.
const (
	Namespace    = "eph"
	NamespaceRef = "refs/namespaces/" + Namespace + "/"
)

func BranchRef(name string, ephemeral bool) string {
	ref := "refs/heads/" + name
	if ephemeral {
		return NamespaceRef + ref
	}
	return ref
}

func TagRef(name string) string { return "refs/tags/" + name }

// SplitRef reports the stripped ref name and whether it was ephemeral.
func SplitRef(full string) (name string, ephemeral bool) {
	if rest, ok := strings.CutPrefix(full, NamespaceRef); ok {
		return rest, true
	}
	return full, false
}

// BranchName extracts the short branch name, or "" if not a branch.
func BranchName(full string) (string, bool) {
	name, _ := SplitRef(full)
	return strings.CutPrefix(name, "refs/heads/")
}
