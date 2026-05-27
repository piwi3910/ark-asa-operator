// Package ark contains pure helpers (no Kubernetes deps) used by the
// ark-asa-operator controllers to compose ARK server configuration.
package ark

// GamePort returns the UDP game port for the map at the given zero-based index,
// relative to the cluster's gamePortStart. Map i lives on port start+i.
func GamePort(start int32, index int) int32 { return start + int32(index) }

// RconPort returns the TCP RCON port for the map at the given zero-based index,
// relative to the cluster's rconPortStart.
func RconPort(start int32, index int) int32 { return start + int32(index) }

// PortConflict reports whether the game port range [gameStart, gameStart+mapCount)
// overlaps the rcon range [rconStart, rconStart+mapCount). Each map consumes exactly
// one game port and one rcon port; with mapCount maps, the game range occupies
// gameStart..gameStart+mapCount-1 inclusive. Used at validation time to fail
// clusters whose gamePortStart + rconPortStart + mapCount would collide.
func PortConflict(gameStart, rconStart int32, mapCount int) bool {
	if mapCount < 1 {
		return false
	}
	gameEnd := gameStart + int32(mapCount) - 1
	rconEnd := rconStart + int32(mapCount) - 1
	// Overlap iff each range's start ≤ the other's end
	return gameStart <= rconEnd && rconStart <= gameEnd
}
