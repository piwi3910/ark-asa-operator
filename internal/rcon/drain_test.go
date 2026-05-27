package rcon

import (
	"context"
	"net"
	"testing"
	"time"
)

// fakeRCONCapture is a fakeRCON variant that captures the exec command body
// into a channel and replies with the provided response.
func fakeRCONCapture(t *testing.T, password string, capture chan<- string, response string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, _, _, body, err := readPacket(conn)
		if err != nil {
			return
		}
		if string(body) != password {
			_ = writePacket(conn, -1, packetTypeAuthResponse, []byte{})
			return
		}
		_ = writePacket(conn, 1, packetTypeAuthResponse, []byte{})
		_, reqID, _, body, err := readPacket(conn)
		if err != nil {
			return
		}
		capture <- string(body)
		_ = writePacket(conn, reqID, packetTypeResponseValue, []byte(response))
	}()
	return ln.Addr().String()
}

func TestAnnounceShutdownEmitsServerChat(t *testing.T) {
	cap := make(chan string, 1)
	addr := fakeRCONCapture(t, "secret", cap, "ok")
	if err := AnnounceShutdown(context.Background(), addr, "secret", 30*time.Minute, "cluster update"); err != nil {
		t.Fatal(err)
	}
	got := <-cap
	want := "ServerChat Server shutting down in 30 minutes for cluster update"
	if got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestSaveAndExitInvokesSaveWorld(t *testing.T) {
	cap := make(chan string, 2)
	addr := fakeRCONCapture(t, "secret", cap, "ok")
	// SaveAndExit issues 2 commands but our fake only captures the first
	if err := SaveAndExit(context.Background(), addr, "secret"); err != nil {
		t.Fatal(err)
	}
	got := <-cap
	if got != "SaveWorld" {
		t.Errorf("first cmd should be SaveWorld, got %q", got)
	}
}

func TestRoundDuration(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{2 * time.Hour, "2 hours"},
		{30 * time.Minute, "30 minutes"},
		{45 * time.Second, "45 seconds"},
	}
	for _, tc := range tests {
		if got := roundDuration(tc.d); got != tc.want {
			t.Errorf("roundDuration(%s) = %q want %q", tc.d, got, tc.want)
		}
	}
}
