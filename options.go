package dfwriter

import (
	"fmt"
	"os"
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
		file:       file,
		maxSize:    10 * 1024 * 1024, // default 10MB
		maxBackups: 5,
	}

	for _, o := range options {
		o(logger)
	}

	return logger, nil
}

// WithMaxBytes returns an option to set the maximum size in bytes before rotation.
func WithMaxBytes(maxBytes int64) func(*DistributedFileWriter) {
	return func(w *DistributedFileWriter) {
		w.maxSize = maxBytes
	}
}

// WithMaxBackups returns an option to set the maximum number of backup files to retain.
func WithMaxBackups(maxBackups int) func(*DistributedFileWriter) {
	return func(w *DistributedFileWriter) {
		w.maxBackups = maxBackups
	}
}

// WithPrefix returns an option to prepend the given byte prefix to each log entry.
func WithPrefix(prefix []byte) func(*DistributedFileWriter) {
	return func(w *DistributedFileWriter) {
		w.prefix = prefix
	}
}

// WithFileLocking returns an option to enable fcntl file-locking during writes.
func WithFileLocking() func(*DistributedFileWriter) {
	return func(w *DistributedFileWriter) {
		w.fsLock = true
	}
}

// WithCompression returns an option to enable gzip compression for the log file.
func WithCompression() func(*DistributedFileWriter) {
	return func(w *DistributedFileWriter) {
		w.compress = true
	}
}
