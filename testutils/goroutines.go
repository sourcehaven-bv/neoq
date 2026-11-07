//go:build testing

package testutils

import (
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// goroutineSettleTimeout bounds how long AssertNoGoroutineLeak waits for
// goroutines that are still exiting after Shutdown returned.
const goroutineSettleTimeout = 5 * time.Second

// goroutinePollInterval is how often settledGoroutineCount samples the count.
const goroutinePollInterval = 20 * time.Millisecond

// AssertNoGoroutineLeak runs cycle n times and fails t unless the goroutine
// count falls back to the count measured before the first of those runs.
// cycle should create a backend and shut it down.
//
// One warm-up run comes first, so goroutines that a dependency starts once per
// process are part of the baseline rather than counted as a leak. Goroutines
// may still be exiting when Shutdown returns, so the count is polled until it
// settles or goroutineSettleTimeout passes.
func AssertNoGoroutineLeak(t *testing.T, n int, cycle func()) {
	t.Helper()

	cycle()
	baseline := settledGoroutineCount(0)

	for range n {
		cycle()
	}

	got := settledGoroutineCount(baseline)
	if got > baseline {
		var stacks strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 1)
		t.Fatalf("%d create/shutdown cycles left %d goroutines running, want at most the baseline of %d\n%s",
			n, got, baseline, stacks.String())
	}
}

// settledGoroutineCount polls the goroutine count until it is at most want, or
// until goroutineSettleTimeout passes, and returns the last count seen. With
// want 0 it returns as soon as the count stops falling.
func settledGoroutineCount(want int) int {
	deadline := time.Now().Add(goroutineSettleTimeout)
	prev := runtime.NumGoroutine()
	for {
		time.Sleep(goroutinePollInterval)
		n := runtime.NumGoroutine()
		if want > 0 && n <= want {
			return n
		}
		if want == 0 && n >= prev {
			return n
		}
		if time.Now().After(deadline) {
			return n
		}
		prev = n
	}
}
