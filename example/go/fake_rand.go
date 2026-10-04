package main

import "math/rand"

// The producer only generates synthetic demo/load-test values, so a
// non-cryptographic RNG is intentional. Keeping every math/rand call here
// confines the gosec G404 suppression to one place.

func randFloat64() float64 {
	return rand.Float64() // #nosec G404 -- synthetic sample data, not security sensitive
}

func randIntn(n int) int {
	return rand.Intn(n) // #nosec G404 -- synthetic sample data, not security sensitive
}
