package rcon

import (
	"context"
	"net"
	"testing"
	"time"
)

// fakeRCON listens, reads one auth packet, answers, then reads one exec and echoes.
func fakeRCON(t *testing.T, password, response string) (addr string, done chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done = make(chan struct{})
	go func() {
		defer close(done)
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		// Auth
		_, _, pktType, body, err := readPacket(conn)
		if err != nil {
			return
		}
		if pktType != packetTypeAuth || string(body) != password {
			_ = writePacket(conn, -1, packetTypeAuthResponse, []byte{})
			return
		}
		_ = writePacket(conn, 1, packetTypeAuthResponse, []byte{})
		// Exec
		_, reqID, _, _, err := readPacket(conn)
		if err != nil {
			return
		}
		_ = writePacket(conn, reqID, packetTypeResponseValue, []byte(response))
	}()
	return ln.Addr().String(), done
}

func TestRCONExec(t *testing.T) {
	addr, _ := fakeRCON(t, "secret", "Players online: 0")
	c, err := Dial(context.Background(), addr, "secret", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.Exec(context.Background(), "ListPlayers")
	if err != nil {
		t.Fatal(err)
	}
	if got != "Players online: 0" {
		t.Errorf("got %q", got)
	}
}

func TestRCONBadPassword(t *testing.T) {
	addr, _ := fakeRCON(t, "real", "x")
	_, err := Dial(context.Background(), addr, "wrong", 2*time.Second)
	if err == nil {
		t.Fatal("expected auth failure")
	}
}
