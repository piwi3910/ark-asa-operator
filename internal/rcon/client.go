// Package rcon implements a minimal Source RCON protocol client over plain TCP.
// No external dependencies. Used by the ark-asa-operator controller to issue
// SaveWorld, DoExit, ServerChat, and ListPlayers against ARK server pods.
//
// Source RCON wire format:
//   int32 length (little-endian, NOT including the length field itself)
//   int32 requestID
//   int32 packetType (3=Auth, 2=ExecCommand, 0=ResponseValue, 2=AuthResponse)
//   string body (null-terminated)
//   byte 0 (extra terminator)
package rcon

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// Packet type constants from the Source RCON spec.
const (
	packetTypeAuth          int32 = 3
	packetTypeAuthResponse  int32 = 2
	packetTypeExecCommand   int32 = 2
	packetTypeResponseValue int32 = 0
)

// ErrAuthFailed is returned by Dial when the server rejects the password.
var ErrAuthFailed = errors.New("rcon: authentication failed")

// Client is an authenticated RCON connection. Not safe for concurrent use.
type Client struct {
	conn   net.Conn
	nextID int32
}

// Dial connects to addr (host:port), authenticates, and returns a Client.
// On bad password returns ErrAuthFailed. timeout applies to both dial and the
// auth round-trip; after Dial returns successfully the client has no deadline
// set (callers should pass a context with deadline to Exec).
func Dial(ctx context.Context, addr, password string, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("rcon: dial: %w", err)
	}
	c := &Client{conn: conn}
	_ = c.conn.SetDeadline(time.Now().Add(timeout))
	id := c.nextRequestID()
	if err := writePacket(conn, id, packetTypeAuth, []byte(password)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_, gotID, _, _, err := readPacket(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if gotID == -1 {
		_ = conn.Close()
		return nil, ErrAuthFailed
	}
	_ = c.conn.SetDeadline(time.Time{})
	return c, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// Exec runs the given RCON command and returns the response body. Uses ctx's
// deadline if set, or a 10s default.
func (c *Client) Exec(ctx context.Context, cmd string) (string, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(10 * time.Second)
	}
	_ = c.conn.SetDeadline(deadline)
	defer func() { _ = c.conn.SetDeadline(time.Time{}) }()
	id := c.nextRequestID()
	if err := writePacket(c.conn, id, packetTypeExecCommand, []byte(cmd)); err != nil {
		return "", err
	}
	_, _, _, body, err := readPacket(c.conn)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (c *Client) nextRequestID() int32 {
	return atomic.AddInt32(&c.nextID, 1)
}

// --- Wire-format helpers (also used by the test fake server) ---

func writePacket(w io.Writer, id, typ int32, body []byte) error {
	// length = id (4) + type (4) + body + 2 null terminators
	length := int32(4 + 4 + len(body) + 2)
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, length); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, id); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, typ); err != nil {
		return err
	}
	buf.Write(body)
	buf.Write([]byte{0, 0})
	_, err := w.Write(buf.Bytes())
	return err
}

func readPacket(r io.Reader) (length, id, typ int32, body []byte, err error) {
	if err = binary.Read(r, binary.LittleEndian, &length); err != nil {
		return
	}
	if length < 10 || length > 4*1024 {
		err = fmt.Errorf("rcon: bad length %d", length)
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &id); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &typ); err != nil {
		return
	}
	body = make([]byte, length-10)
	if _, err = io.ReadFull(r, body); err != nil {
		return
	}
	terminator := make([]byte, 2)
	if _, err = io.ReadFull(r, terminator); err != nil {
		return
	}
	return
}
