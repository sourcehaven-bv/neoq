package handler_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acaloiaro/neoq/handler"
)

const testTimeout = 50 * time.Millisecond

// TestExecTimeoutCancelsHandler checks that a handler which honors its context is canceled when JobTimeout is
// exceeded, and that Exec does not return before the handler has stopped.
func TestExecTimeoutCancelsHandler(t *testing.T) {
	var stopped atomic.Bool
	h := handler.New("q", func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // simulate cleanup, such as aborting a query
		stopped.Store(true)
		return ctx.Err()
	}, handler.JobTimeout(testTimeout))

	execExpectTimeout(t, h)

	if !stopped.Load() {
		t.Fatal("Exec returned before the canceled handler stopped")
	}
}

// TestExecTimeoutDoesNotLeakGoroutine checks that a handler which finishes after JobTimeout does not leave its
// goroutine blocked.
func TestExecTimeoutDoesNotLeakGoroutine(t *testing.T) {
	release := make(chan struct{})
	h := handler.New("q", func(_ context.Context) error {
		<-release
		return nil
	}, handler.JobTimeout(testTimeout))

	execExpectTimeout(t, h)
	close(release)

	assertExecGoroutineExits(t)
}

// TestExecTimeoutThenPanicDoesNotLeakGoroutine checks that a handler which panics after JobTimeout is recovered and
// does not leave its goroutine blocked.
func TestExecTimeoutThenPanicDoesNotLeakGoroutine(t *testing.T) {
	release := make(chan struct{})
	recovered := make(chan struct{})
	h := handler.New("q", func(_ context.Context) error {
		<-release
		panic("boom")
	},
		handler.JobTimeout(testTimeout),
		handler.RecoverCallback(func(_ context.Context, _ error) error {
			close(recovered)
			return nil
		}),
	)

	execExpectTimeout(t, h)
	close(release)

	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("recovery callback was not called")
	}

	assertExecGoroutineExits(t)
}

// TestExecTimeoutDoesNotWaitForeverForHandler checks that a handler which ignores its context does not block Exec
// for longer than the grace period.
func TestExecTimeoutDoesNotWaitForeverForHandler(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h := handler.New("q", func(_ context.Context) error {
		<-release
		return nil
	}, handler.JobTimeout(testTimeout))

	start := time.Now()
	execExpectTimeout(t, h)

	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Exec blocked for %s on a handler that ignores its context", elapsed)
	}
}

// execExpectTimeout runs h and fails the test unless it exceeded its JobTimeout.
func execExpectTimeout(t *testing.T, h handler.Handler) {
	t.Helper()

	err := handler.Exec(context.Background(), h)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
}

// assertExecGoroutineExits fails the test if a goroutine started by handler.Exec is still alive after a second.
func assertExecGoroutineExits(t *testing.T) {
	t.Helper()

	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(time.Second)
	for {
		stack := buf[:runtime.Stack(buf, true)]
		if !bytes.Contains(stack, []byte("handler.Exec.func")) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("handler goroutine is still running:\n%s", stack)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
