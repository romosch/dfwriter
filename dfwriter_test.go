package dfwriter

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLoggerWritesAndPrefixes verifies that the logger writes messages with the correct prefix
// and ensures the written content matches the expected output.
func TestLoggerWritesAndPrefixes(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	prefix := []byte("[TEST] ")
	logger, err := New(logPath,
		WithMaxBytes(1024),
		WithMaxBackups(5),
		WithPrefix(prefix),
	)
	assert.NoError(t, err)

	message := "hello log\n"
	n, err := logger.Write([]byte(message))
	assert.NoError(t, err)
	assert.Equal(t, len(message), n)

	err = logger.Sync()
	assert.NoError(t, err)
	err = logger.Close()
	assert.NoError(t, err)

	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	assert.Equal(t, string(contents), string(prefix)+message)
}

// TestLoggerWritesAndPrefixes verifies that the logger writes messages with the correct prefix
// and ensures the written content matches the expected output.
func TestLoggerPartialWrites(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	prefix := []byte("[TEST] ")
	logger, err := New(logPath,
		WithMaxBytes(1024),
		WithMaxBackups(5),
		WithPrefix(prefix),
	)
	assert.NoError(t, err)

	message1 := "foo "
	message2 := "bar\n"
	message3 := "baz"
	n, err := logger.Write([]byte(message1))
	assert.NoError(t, err)
	assert.Equal(t, len(message1), n)
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	assert.Empty(t, contents)

	n, err = logger.Write([]byte(message2))
	assert.NoError(t, err)
	assert.Equal(t, len(message2), n)
	contents, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	assert.Equal(t, string(contents), "[TEST] foo bar\n")

	n, err = logger.Write([]byte(message3))
	assert.NoError(t, err)
	assert.Equal(t, len(message3), n)
	contents, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	assert.Equal(t, "[TEST] foo bar\n", string(contents))

	err = logger.Sync()
	assert.NoError(t, err)
	err = logger.Close()
	assert.NoError(t, err)
	contents, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	assert.Equal(t, "[TEST] foo bar\n[TEST] baz", string(contents))
}

// TestLineExceedsMaxSize ensures that attempting to write a line larger than the maximum size
// results in an error and no data is written to the log file.
func TestLineExceedsMaxSize(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "exceed.log")
	logger, err := New(logPath,
		WithMaxBytes(100),
		WithMaxBackups(5),
	)
	assert.NoError(t, err)

	longMessage := strings.Repeat("x", 200) + "\n"
	n, err := logger.Write([]byte(longMessage))
	assert.Error(t, err)
	assert.Equal(t, 0, n)

	err = logger.Sync()
	assert.Error(t, err)
	err = logger.Close()
	assert.NoError(t, err)

	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}
	assert.Equal(t, len(contents), 0)
}

// TestLogRotationOccurs verifies that log rotation occurs when the maximum file size is exceeded.
func TestLogRotationOccurs(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "rotate.log")
	prefix := []byte("[ROTATE] ")
	logger, err := New(logPath,
		WithMaxBytes(100),
		WithMaxBackups(5),
		WithPrefix(prefix),
	)
	assert.NoError(t, err)

	// Write enough data to exceed 100 bytes
	for i := 0; i < 10; i++ {
		msg := strings.Repeat("x", 15) + "\n"
		n, err := logger.Write([]byte(msg))
		assert.NoError(t, err)
		assert.Equal(t, len(msg), n)
	}

	err = logger.Sync()
	assert.NoError(t, err)
	err = logger.Close()
	assert.NoError(t, err)

	files, err := filepath.Glob(logPath + ".*")
	if err != nil {
		t.Fatalf("failed to list rotated files: %v", err)
	}

	if len(files) == 0 {
		t.Error("expected rotated log file, found none")
	}
}

// TestMaxBackupsIsEnforced ensures that the maximum number of backup files is enforced.
func TestMaxBackupsIsEnforced(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "maxbackup.log")
	prefix := []byte("[MAX] ")
	logger, err := New(logPath,
		WithMaxBytes(100),
		WithMaxBackups(3),
		WithPrefix(prefix),
	)
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		msg := strings.Repeat("y", 80) + "\n"
		_, err := logger.Write([]byte(msg))
		assert.NoError(t, err)
	}

	files, err := filepath.Glob(logPath + ".*")
	assert.NoError(t, err)

	assert.Equal(t, 3, len(files), "expected 3 backups, found %d", len(files))
}

// TestRotationLinesRetained ensures that all log lines are retained across rotated files.
func TestRotationLinesRetained(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "file.log")

	const lineCount = 50
	const lineSize = 15
	const rotationSize = 100

	logger, err := New(logPath,
		WithMaxBytes(rotationSize),
		WithMaxBackups(100),
		WithFileLocking(),
	)
	assert.NoError(t, err)

	for i := 0; i < lineCount; i++ {
		msg := strings.Repeat("y", lineSize) + "\n"
		_, err := logger.Write([]byte(msg))
		assert.NoError(t, err)
	}

	files, err := filepath.Glob(logPath + "*")
	assert.NoError(t, err)

	nLogFiles := lineCount/(rotationSize/lineSize) + 1
	assert.Equal(t, nLogFiles, len(files), "expected %d logfiles, found %d", nLogFiles, len(files))

	total := 0

	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		total += len(bytes.Split(data, []byte("\n"))) - 1
	}

	assert.Equal(t, lineCount, total, "expected %d lines in all files, got %d", lineCount, total)
}

// TestCompression verifies that log files are compressed after rotation.
func TestCompression(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "compress.log")
	const lineCount = 8
	const lineSize = 15
	const rotationSize = 100

	logger, err := New(logPath,
		WithMaxBytes(rotationSize),
		WithMaxBackups(100),
		WithCompression(),
		WithFileLocking(),
	)
	assert.NoError(t, err)

	for i := 0; i < lineCount; i++ {
		msg := strings.Repeat("y", lineSize) + "\n"
		_, err := logger.Write([]byte(msg))
		assert.NoError(t, err)
	}

	err = logger.Sync()
	assert.NoError(t, err)
	err = logger.Close()
	assert.NoError(t, err)

	files, err := filepath.Glob(logPath + ".*")
	if err != nil {
		t.Fatalf("failed to list rotated files: %v", err)
	}

	assert.Equal(t, 1, len(files), "expected 1 compressed log file, found %d", len(files))

	gzFile, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("failed to list rotated files: %v", err)
	}
	defer gzFile.Close()

	// Create a gzip.Reader
	gr, err := gzip.NewReader(gzFile)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gr.Close()

	decompressedData, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("Failed to read decompressed data: %v", err)
	}
	assert.Equal(t, (lineSize+1)*(rotationSize/lineSize), len(decompressedData), "expected %d bytes, got %d", lineSize*(rotationSize/lineSize), len(decompressedData))
	lines := bytes.Split(decompressedData, []byte("\n"))
	assert.Equal(t, rotationSize/lineSize, len(lines)-1, "expected %d lines, got %d", rotationSize/lineSize, len(lines)-1)
}

func TestConcurrentWritesAndRotation(t *testing.T) {
	// Parent mode: spawn N child processes
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")

	const (
		writers        = 10
		linesPerWriter = 300
		lineSize       = 10
		rotationSize   = 5000
	)

	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			prefix := strconv.Itoa(id)
			logger, err := New(
				logPath,
				WithMaxBytes(rotationSize),
				WithMaxBackups(10000),
				WithPrefix([]byte(prefix)),
				WithFileLocking(),
			)
			assert.NoError(t, err)
			defer logger.Close()

			msg := []byte(strings.Repeat("x", lineSize-len(prefix)-1) + "\n")
			for range linesPerWriter {
				_, err := logger.Write(msg)
				assert.NoError(t, err)
			}
			err = logger.Sync()
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// now validate total lines
	files, _ := filepath.Glob(logPath + "*")
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

func BenchmarkLoggerWrite(b *testing.B) {
	tmpDir := b.TempDir()
	logPath := filepath.Join(tmpDir, "benchmark.log")
	logger, err := New(logPath,
		WithFileLocking(),
		WithMaxBytes(100*1024*1024), // 100MB
	)
	if err != nil {
		b.Fatalf("failed to create logger: %v", err)
	}
	defer logger.Close()

	message := []byte("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.\n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := logger.Write(message)
		if err != nil {
			b.Fatalf("failed to write log: %v", err)
		}
	}
}
