// Package main is a tiny TCP server that pretends to be an ARK SA dedicated
// server for CI e2e tests. Listens on $RCON_PORT for the Source RCON protocol;
// ignores the UDP game port. Accepts any auth and echoes "ok" to commands.
package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
)

func main() {
	rconPort := os.Getenv("RCON_PORT")
	if rconPort == "" {
		rconPort = "27020"
	}
	log.Printf("fake-ark-server: listening RCON on :%s, SESSION_NAME=%q SERVER_MAP=%q",
		rconPort, os.Getenv("SESSION_NAME"), os.Getenv("SERVER_MAP"))

	ln, err := net.Listen("tcp", ":"+rconPort)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go serve(conn)
	}
}

func serve(conn net.Conn) {
	defer conn.Close()
	for {
		var length, id, typ int32
		if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
			return
		}
		if length < 10 || length > 4096 {
			return
		}
		_ = binary.Read(conn, binary.LittleEndian, &id)
		_ = binary.Read(conn, binary.LittleEndian, &typ)
		body := make([]byte, length-10)
		_, _ = io.ReadFull(conn, body)
		term := make([]byte, 2)
		_, _ = io.ReadFull(conn, term)
		// Accept any auth (typ 3) and echo "ok" to commands (typ 2)
		respID := id
		if typ == 3 {
			respID = 1
		}
		writePkt(conn, respID, 0, []byte("ok"))
	}
}

func writePkt(w io.Writer, id, typ int32, body []byte) {
	length := int32(4 + 4 + len(body) + 2)
	_ = binary.Write(w, binary.LittleEndian, length)
	_ = binary.Write(w, binary.LittleEndian, id)
	_ = binary.Write(w, binary.LittleEndian, typ)
	_, _ = w.Write(body)
	_, _ = w.Write([]byte{0, 0})
}
