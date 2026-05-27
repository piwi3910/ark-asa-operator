package rcon

import (
	"context"
	"fmt"
	"time"
)

// AnnounceShutdown emits a single ServerChat warning to all connected players.
// Idempotent at the protocol level — callers decide whether to re-emit.
func AnnounceShutdown(ctx context.Context, addr, password string, in time.Duration, reason string) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c, err := Dial(dialCtx, addr, password, 5*time.Second)
	if err != nil {
		return fmt.Errorf("rcon dial: %w", err)
	}
	defer c.Close()
	msg := fmt.Sprintf("ServerChat Server shutting down in %s for %s", roundDuration(in), reason)
	_, err = c.Exec(ctx, msg)
	return err
}

// SaveAndExit invokes SaveWorld then DoExit. Best-effort: SaveWorld errors are
// returned; DoExit's response is ignored (it causes the connection to drop).
func SaveAndExit(ctx context.Context, addr, password string) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c, err := Dial(dialCtx, addr, password, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.Exec(ctx, "SaveWorld"); err != nil {
		return fmt.Errorf("SaveWorld: %w", err)
	}
	_, _ = c.Exec(ctx, "DoExit") // connection drops; ignore err
	return nil
}

func roundDuration(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%.0f hours", d.Hours())
	case d >= time.Minute:
		return fmt.Sprintf("%.0f minutes", d.Minutes())
	default:
		return fmt.Sprintf("%.0f seconds", d.Seconds())
	}
}
