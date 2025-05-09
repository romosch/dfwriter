package dfwriter

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type DistributedFileWriter struct {
	fsLock     bool
	compress   bool
	maxBackups int
	maxSize    int64
	file       *os.File
	prefix     []byte
	buf        bytes.Buffer
}

// Write buffers input until newline and writes each complete line with rotation and file-locking.
func (w *DistributedFileWriter) Write(b []byte) (int, error) {
	for i := range b {
		w.buf.WriteByte(b[i])
		if b[i] == '\n' {
			err := w.writeLine(w.buf.Bytes())
			if err != nil {
				return 0, err
			}
		}
	}

	return len(b), nil
}

// writeLine handles writing a single log line with optional prefix, rotation, and file locking.
// writeLine writes a single line to the log file managed by the SafeRotatingLogger.
// It ensures that the line adheres to the maximum size constraints and handles
// file rotation if necessary. The function also manages file locking to ensure
// atomic writes and prevent data corruption in concurrent environments.
//
// Parameters:
//   - line: A byte slice representing the line to be written. If the line is empty,
//     the function returns immediately without performing any operations.
//
// Returns:
//   - err: An error if the operation fails. Possible error scenarios include:
//   - The line exceeds the maximum allowed size.
//   - Failure to acquire file locks.
//   - Failure to rotate the log file.
//   - Failure to write to the file or sync the data.
//
// Behavior:
//   - If the line length (including the prefix) exceeds the maximum size (maxSize),
//     the function returns an error.
//   - The function checks if the log file needs to be rotated based on the current
//     file size and the size of the new line. If rotation is required, it performs
//     the rotation before writing the line.
//   - File locking is used to ensure atomic writes. If the line size exceeds the
//     system's PIPE_BUF size or rotation is required, an exclusive lock is acquired.
//     Otherwise, a shared lock is used.
//   - After writing the line, the function ensures the file is synced to disk and
//     releases the lock.
//
// Notes:
//   - On Unix-like systems, writes to a file descriptor are atomic if the size of
//     the write is less than or equal to the system's PIPE_BUF size.
//   - The function appends the logger's prefix to the line before writing it to the file.
func (w *DistributedFileWriter) writeLine(line []byte) (err error) {
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
		if n > PIPE_BUF || shouldRotate {
			if err := w.lockFileWrite(); err != nil {
				return fmt.Errorf("failed to acquire lock on %s: %w", w.file.Name(), err)
			}
			// Check again if we need to rotate after acquiring the write-lock
			shouldRotate, err = w.shouldRotate(n)
			if err != nil {
				return err
			}
		} else {
			if err := w.lockFileRead(); err != nil {
				return fmt.Errorf("failed to acquire lock on %s: %w", w.file.Name(), err)
			}
		}
		defer func() {
			if n > PIPE_BUF || shouldRotate {
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
			unlockErr := w.unlockFile()
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

	if len(backups) <= w.maxBackups {
		return nil
	}

	sort.Strings(backups)
	toDelete := backups[:len(backups)-w.maxBackups]
	for _, file := range toDelete {
		_ = os.Remove(file) // best-effort
	}

	return nil
}

// Close closes the underlying log file.
func (w *DistributedFileWriter) Close() error {
	return w.file.Close()
}

// Sync writes any remaining buffered data as a complete log entry.
func (w *DistributedFileWriter) Sync() error {
	if w.buf.Len() != 0 {
		// Write the remaining buffer content with the prefix
		return w.writeLine(w.buf.Bytes())
	}

	return w.file.Sync()
}

func (w *DistributedFileWriter) Name() string {
	return w.file.Name()
}

func (w *DistributedFileWriter) lockFileWrite() error {
	flock := syscall.Flock_t{
		Type:   syscall.F_WRLCK, // Write lock (exclusive)
		Whence: 0,               // From start of the file
		Start:  0,
		Len:    0, // Lock the entire file
	}
	return syscall.FcntlFlock(w.file.Fd(), syscall.F_SETLKW, &flock)
}

// Acquire a read lock (shared lock)
func (w *DistributedFileWriter) lockFileRead() error {
	flock := syscall.Flock_t{
		Type:   syscall.F_RDLCK, // Read lock (shared)
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	return syscall.FcntlFlock(w.file.Fd(), syscall.F_SETLKW, &flock)
}

// Unlock the file
func (w *DistributedFileWriter) unlockFile() error {
	flock := syscall.Flock_t{
		Type:   syscall.F_UNLCK, // Unlock
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	return syscall.FcntlFlock(w.file.Fd(), syscall.F_SETLK, &flock)
}

func (w *DistributedFileWriter) shouldRotate(n int) (bool, error) {
	stat, err := w.file.Stat()
	if err != nil {
		return false, err
	}

	return stat.Size()+int64(n) >= w.maxSize && w.maxSize > 0, nil
}
