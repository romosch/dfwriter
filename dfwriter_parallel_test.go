package dfwriter

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConcurrentWritesAndRotation ensures that concurrent writes and log rotation work correctly.
// It spawns multiple processes, each writing to the same log file.
// The test checks that the total number of lines written across all files matches the expected count (no interleaving).
// It also verifies that the log files are correctly rotated based on the specified rotation size.

// The test uses a helper binary to perform the actual writing, which is built from a separate Go source file.
// This is to simulate processes on different machines writing to the same log file using fcntl locking.

func TestConcurrentWritesAndRotation(t *testing.T) {
	// build helper binary into temp dir
	out := filepath.Join(t.TempDir(), "logwriter")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/test/main.go")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build helper: %v\n", err)
		os.Exit(1)
	}

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if f, err := os.Create(logPath); err != nil {
		t.Fatalf("create log: %v", err)
	} else {
		f.Close()
	}

	const (
		writers        = 5
		linesPerWriter = 20
		lineSize       = 10
		rotationSize   = 100
	)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		cmd := exec.Command(
			out,
			"-log="+logPath,
			"-prefix="+strconv.Itoa(i),
			"-lines="+strconv.Itoa(linesPerWriter),
			"-lineSize="+strconv.Itoa(lineSize),
			"-rotationSize="+strconv.Itoa(rotationSize),
			"-lock",
		)
		// inherit environment
		cmd.Env = os.Environ()
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cmd.Start(); err != nil {
				fmt.Printf("failed to start child %d: %v\n", i, err)
			}
			if err := cmd.Wait(); err != nil {
				fmt.Printf("child %d failed: %v\n", i, err)
			}
		}()
	}
	wg.Wait()

	// now validate total lines
	files, _ := filepath.Glob(logPath + "*")
	assert.GreaterOrEqual(t, len(files), (writers*linesPerWriter*lineSize)/rotationSize)
	total := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		total += len(bytes.Split(data, []byte("\n"))) - 1

	}
	want := writers * linesPerWriter
	assert.Equal(t, want, total, "expected %d lines in all files, got %d", want, total)
}
