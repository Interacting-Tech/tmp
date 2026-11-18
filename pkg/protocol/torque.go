package protocol

import (
	"math"
	"strings"
)

// NormalizeHand returns lowercase trimmed hand without heap allocation on common inputs.
func NormalizeHand(hand string) string {
	switch hand {
	case "", HandLeft, HandRight:
		return hand
	case "LEFT", "Left":
		return HandLeft
	case "RIGHT", "Right":
		return HandRight
	default:
		trimmed := strings.TrimSpace(hand)
		if trimmed == "" {
			return ""
		}
		return strings.ToLower(trimmed)
	}
}

// AreOppositeHands reports whether one player uses "left" and the other uses "right" (PROTOCOL.md section 4.3).
func AreOppositeHands(handA, handB string) bool {
	a := NormalizeHand(handA)
	b := NormalizeHand(handB)
	return (a == HandLeft && b == HandRight) || (a == HandRight && b == HandLeft)
}

// DoesServerFlipOpponentTorque matches the exact server flip condition across PROTOCOL.md
// sections 4.3 and 4.4: flips when one slot is "left" and the other is not "left"
// (which flips "left" vs "right" AND produces {"opponent_torque":-0.0} on disconnect when remaining is "left").
func DoesServerFlipOpponentTorque(handA, handB string) bool {
	aLeft := NormalizeHand(handA) == HandLeft
	bLeft := NormalizeHand(handB) == HandLeft
	return aLeft != bLeft
}

// ApplyOpponentTorqueSign applies the section 4.3 sign rule between two hands.
func ApplyOpponentTorqueSign(opponentTorque float64, handA, handB string) float64 {
	if AreOppositeHands(handA, handB) {
		return SanitizeFloat(-opponentTorque)
	}
	return SanitizeFloat(opponentTorque)
}

// UndoLocalServerFlip reverses the local server's flip on received opponent_torque so the
// physical machine's original telemetry value is recovered without double-negating on a remote server.
func UndoLocalServerFlip(relayedOpponentTorque float64, localHand, lastSentRivalHand string) float64 {
	if DoesServerFlipOpponentTorque(localHand, lastSentRivalHand) {
		return SanitizeFloat(-relayedOpponentTorque)
	}
	return SanitizeFloat(relayedOpponentTorque)
}

// SanitizeFloat replaces NaN/Inf with 0.0 and normalizes IEEE -0.0 to +0.0 so JSON serialization never fails.
func SanitizeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v == 0 {
		return 0
	}
	return v
}
