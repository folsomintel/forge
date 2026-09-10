package githttp

import "testing"

func TestSSHServiceScope(t *testing.T) {
	cases := []struct {
		in      string
		service string
		arg     string
		ok      bool
	}{
		{"git-upload-pack '/my-repo.git'", "git-upload-pack", "'/my-repo.git'", true},
		{"git-receive-pack 'r.git'", "git-receive-pack", "'r.git'", true},
		{"git upload-pack 'x'", "git-upload-pack", "'x'", true},
		{"scp -t /etc/passwd", "", "", false},
		{"rm -rf /", "", "", false},
		{"", "", "", false},
	}
	for _, c := range cases {
		svc, arg, ok := SSHServiceScope(c.in)
		if ok != c.ok || svc != c.service || arg != c.arg {
			t.Errorf("SSHServiceScope(%q) = (%q,%q,%v), want (%q,%q,%v)",
				c.in, svc, arg, ok, c.service, c.arg, c.ok)
		}
	}
}

func TestParseSSHTarget(t *testing.T) {
	cases := []struct {
		in, repo, ns string
	}{
		{"'/my-repo.git'", "my-repo", ""},
		{"r.git", "r", ""},
		{"'scratch+ephemeral.git'", "scratch", "eph"},
		{"/deep/name.git", "deep/name", ""},
	}
	for _, c := range cases {
		repo, ns := parseSSHTarget(c.in)
		if repo != c.repo || ns != c.ns {
			t.Errorf("parseSSHTarget(%q) = (%q,%q), want (%q,%q)", c.in, repo, ns, c.repo, c.ns)
		}
	}
}
