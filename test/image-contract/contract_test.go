// Package contract validates that the upstream sknnr/ark-ascended-server image
// still meets the env-var contract the ark-asa-operator depends on. Skipped in
// short mode; run via `make image-contract`.
package contract

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestImageContractEnvNames(t *testing.T) {
	if testing.Short() {
		t.Skip("image-contract test skipped in -short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--entrypoint=env",
		"docker.io/sknnr/ark-ascended-server:latest").CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	got := string(out)
	for _, name := range []string{"ARK_PATH", "GE_PROTON_VERSION", "STEAM_PATH"} {
		if !strings.Contains(got, name+"=") {
			t.Errorf("upstream image dropped expected env var %q. Update operator or pin image tag.", name)
		}
	}
}
