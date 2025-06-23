package dfwriter

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

type DistributedFileWriter struct {
	fsLock         bool
	compress       bool
	maxBackups     int
	maxSize        int64
	atomicLineSize int
	file           *os.File
	maxAge         time.Duration
	prefix         []byte
	buf            bytes.Buffer
}

// Write buffers the given bytes. If a newline is encountered, the buffer
// contents are written to the file via the WriteLine method.
// Returns the number of bytes buffered and any error encountered.
func (w *DistributedFileWriter) Write(b []byte) (int, error) {
	for i := range b {
		w.buf.WriteByte(b[i])
		if b[i] == '\n' {
			err := w.WriteLine(w.buf.Bytes())
			if err != nil {
				return 0, err
			}
		}
	}

	return len(b), nil
}

// WriteLine writes the given bytes to the file, prepending the prefix if set.
// It handles rotation if the line exceeds the max size and manages file locking
// to ensure atomic writes. Returns any error encountered.
func (w *DistributedFileWriter) WriteLine(line []byte) (err error) {
	if len(line) == 0 {
		return nil
	}
	n := len(line) + len(w.prefix)
	if int64(n) > w.maxSize && w.maxSize > 0 {
		return fmt.Errorf("line exceeds max size")
	}

	shouldRotate, err := w.shouldRotate(n)
	if err != nil {
		return err
	}

	// If the line is larger than PIPE_BUF, we need to acquire an exclusive lock
	// to ensure atomic writes. Otherwise, we can use a shared lock.
	// On Unix-like systems, writes to a file descriptor are atomic if the size
	// of the write is less than or equal to the system’s PIPE_BUF size
	if w.fsLock {
		if n > w.atomicLineSize || shouldRotate {
			if err := syscall.Flock(int(w.file.Fd()), syscall.LOCK_EX); err != nil {
				return fmt.Errorf("failed to acquire exclusive lock on %s: %w", w.file.Name(), err)
			}
			// Check again if we need to rotate after acquiring the write-lock
			shouldRotate, err = w.shouldRotate(n)
			if err != nil {
				return err
			}
		} else {
			if err := syscall.Flock(int(w.file.Fd()), syscall.LOCK_SH); err != nil {
				return fmt.Errorf("failed to acquire shared lock on %s: %w", w.file.Name(), err)
			}
		}
		defer func() {
			if n > w.atomicLineSize || shouldRotate {
				// Sync the file to ensure all data is written before unlocking
				syncErr := w.file.Sync()
				if syncErr != nil {
					syncErr = fmt.Errorf("failed to sync %s: %w", w.file.Name(), syncErr)
					if err != nil {
						err = fmt.Errorf("%w; %w", err, syncErr)
					} else {
						err = syncErr
					}
				}
			}
			// Unlock the file after writing
			unlockErr := syscall.Flock(int(w.file.Fd()), syscall.LOCK_UN)
			if unlockErr != nil {
				unlockErr = fmt.Errorf("failed to unlock %s: %w", w.file.Name(), unlockErr)
				if err != nil {
					err = fmt.Errorf("%w; %w", err, unlockErr)
				} else {
					err = unlockErr
				}
			}
		}()
	}

	if shouldRotate {
		err = w.rotate()
		if err != nil {
			return err
		}
	}

	line = append(w.prefix, line...)

	_, err = w.file.Write(line)
	if err != nil {
		return err
	}

	w.buf.Truncate(0)

	return nil
}

// rotate creates a timestamped backup of the current log file, truncates the original, and cleans up old backups.
func (w *DistributedFileWriter) rotate() error {
	i := 0
	timestamp := time.Now().Format("20060102-150405")
	backupPath := fmt.Sprintf("%s.%s.%d", w.file.Name(), timestamp, i)
	if w.compress {
		backupPath += ".gz"
	}

	// Check if a file with the same backupPath already exists

	_, err := os.Stat(backupPath)
	for err == nil {
		// Increment the backup number
		i++
		backupPath = fmt.Sprintf("%s.%s.%d", w.file.Name(), timestamp, i)
		_, err = os.Stat(backupPath)
	}

	var backupFile io.WriteCloser

	// 1) Create the backup file
	if w.compress {
		outFile, err := os.Create(backupPath)
		if err != nil {
			return err
		}
		defer outFile.Close()
		// Create a gzip.Writer on top of the file writer
		backupFile = gzip.NewWriter(outFile)
	} else {
		// Create a regular file writer (no compression)
		backupFile, err = os.Create(backupPath)
		if err != nil {
			return err
		}
	}
	defer backupFile.Close()

	// 2) Open the log for reading only
	srcFile, err := os.Open(w.file.Name()) // O_RDONLY
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// 3) Copy everything into the backup
	if _, err := io.Copy(backupFile, srcFile); err != nil {
		return err
	}

	// 4) Sync the backup file to ensure all data is written
	err = w.file.Sync()
	if err != nil {
		return err
	}

	// 5) Truncate your append-only writer
	if err := w.file.Truncate(0); err != nil {
		return err
	}

	return w.cleanupOldBackups()
}

// cleanupOldBackups deletes oldest backup files to enforce the maxBackups limit.
func (w *DistributedFileWriter) cleanupOldBackups() error {
	matches, err := filepath.Glob(w.file.Name() + ".*")
	if err != nil {
		return err
	}

	var backups []string
	for _, file := range matches {
		if strings.HasPrefix(file, w.file.Name()+".") && len(file) > len(w.file.Name())+1 {
			backups = append(backups, file)
		}
	}

	sort.Strings(backups)
	for i, file := range backups {
		expired, err := w.isOlderThanFilename(file)
		if err != nil {
			return err
		}
		if (len(backups)-i > w.maxBackups && w.maxBackups > 0) || expired {
			err = os.Remove(file)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// isOlderThanFilename returns true if the embedded timestamp in fname
// (in the form ".log.YYYYMMDD-HHMMSS.") is before cutoff.
func (w *DistributedFileWriter) isOlderThanFilename(fname string) (bool, error) {
	if w.maxAge <= 0 {
		return false, nil
	}
	re := regexp.MustCompile(`\.log\.(\d{8}-\d{6})\.`)
	matches := re.FindStringSubmatch(fname)
	if len(matches) < 2 {
		return false, fmt.Errorf("no timestamp found in %q", fname)
	}

	ts, err := time.Parse("20060102-150405", matches[1])
	if err != nil {
		return false, fmt.Errorf("cannot parse timestamp %q: %w", matches[1], err)
	}
	cutoff := time.Now().Add(-w.maxAge)

	return ts.Before(cutoff), nil
}

// Close calls the Sync function and then closes the underlying log file.
func (w *DistributedFileWriter) Close() error {
	syncErr := w.Sync()
	closeErr := w.file.Close()
	if syncErr != nil && closeErr != nil {
		return fmt.Errorf("failed to sync and close file: %w; %w", syncErr, closeErr)
	} else if syncErr != nil {
		return fmt.Errorf("failed to sync file: %w", syncErr)
	} else if closeErr != nil {
		return fmt.Errorf("failed to close file: %w", closeErr)
	}
	return nil
}

// Sync writes any remaining buffered data as a complete log entry.
func (w *DistributedFileWriter) Sync() error {
	if w.buf.Len() != 0 {
		// Write the remaining buffer content with the prefix
		return w.WriteLine(w.buf.Bytes())
	}

	return w.file.Sync()
}

// Name returns the name of the log file.
func (w *DistributedFileWriter) Name() string {
	return w.file.Name()
}

func (w *DistributedFileWriter) shouldRotate(n int) (bool, error) {
	stat, err := w.file.Stat()
	if err != nil {
		return false, err
	}

	return stat.Size()+int64(n) >= w.maxSize && w.maxSize > 0, nil
}
