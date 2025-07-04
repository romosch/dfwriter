package dfwriter

import (
	"fmt"
	"os"
	"time"
)

type Option func(*DistributedFileWriter)

// New creates a new DistributedFileWriter writing to the specified fileName.
// It opens or creates the log file and applies functional options for configuration.
// The file is opened in append mode, and the file permissions are set to the same as the existing file if it exists.
// If the file does not exist, it is created with default permissions (0644).

func New(fileName string, options ...Option) (*DistributedFileWriter, error) {
	mode := os.FileMode(0644)
	info, err := os.Stat(fileName)
	if err == nil {
		mode = info.Mode()
	}

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, mode)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}

	logger := &DistributedFileWriter{
		file:           file,
		atomicLineSize: 4096, // Default atomic line size for most unix systems
	}

	for _, o := range options {
		o(logger)
	}

	return logger, nil
}

// WithMaxBytes returns an option to set the maximum size in bytes before rotation.
func WithMaxBytes(maxBytes int64) Option {
	return func(w *DistributedFileWriter) {
		w.maxSize = maxBytes
	}
}

// WithMaxBackups returns an option to set the maximum number of backup files to retain.
func WithMaxBackups(maxBackups int) Option {
	return func(w *DistributedFileWriter) {
		w.maxBackups = maxBackups
	}
}

// WithPrefix returns an option to prepend the given byte prefix to each log entry.
func WithPrefix(prefix []byte) Option {
	return func(w *DistributedFileWriter) {
		w.prefix = prefix
	}
}

// WithPrefix returns an option to prepend the given byte prefix to each log entry.
func WithMaxAge(age time.Duration) Option {
	return func(w *DistributedFileWriter) {
		w.maxAge = age
	}
}

// WithFileLocking returns an option to enable filesystem file-locking during writes.
func WithFileLocking() Option {
	return func(w *DistributedFileWriter) {
		w.fsLock = true
	}
}

// WithCompression returns an option to enable gzip compression for generated backup log files.
func WithCompression() Option {
	return func(w *DistributedFileWriter) {
		w.compress = true
	}
}

// WithAtomicLineSize returns an option to set the size in bytes assumed to be atomic for writes to a file.
// If a line exceeds this size, an exclusive lock is acquired for writing it to ensure atomicity.
func WithAtomicLineSize(size int) Option {
	return func(w *DistributedFileWriter) {
		w.atomicLineSize = size
	}
}
