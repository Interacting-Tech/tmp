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

// SanitizeFloat replaces NaN/Inf with 0.0 and normalizes IEEE -0.0 to +0.0 so JSON serialization never fails.
func SanitizeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v == 0 {
		return 0
	}
	return v
}
