# dfwriter

 is a Go package providing a distributed file-based log writer. It relies on file-locking and POSIX atomic file-descriptor writes guarantees to
 log to a single file from multiple processes without interleaving, and thread-safe rotation.

## Features

- Automatic log rotation when the file exceeds a configurable size limit
- Configurable number of backup log files to retain
- Optional prefix for each log line
- Optional file-level locking for safe concurrent writes and rotation
- Flush support to write buffered data before exit

## Installation

Install with `go get`:

    go get dfwriter

## Usage

```go
package main

import (
    "log"
    "dfwriter"
)

func main() {
    // Create a new writer writing to "app.log"
    writer, err := dfwriter.New("app.log",
        dfwriter.WithMaxBytes(5*1024*1024),   // rotate at 5MB
        dfwriter.WithMaxBackups(3),            // keep up to 3 backups
        dfwriter.WithPrefix([]byte("[SERVER-1] ")), // prefix each entry
        dfwriter.WithFileLocking(),            // enable file locking
    )
    if err != nil {
        log.Fatalf("failed to create writer: %v", err)
    }
    defer writer.Close()

    // Write a log entry (must end with newline to trigger write)
    _, err = writer.Write([]byte("Application started\n"))
    if err != nil {
        log.Printf("write error: %v", err)
    }

    // Flush any remaining buffered data
    if err := writer.Flush(); err != nil {
        log.Printf("flush error: %v", err)
    }
}
```

## API Reference

### New(fileName string, options ...Option) (*DistributedFileWriter, error)

Creates a new `DistributedFileWriter` that writes to the specified `fileName`.  It opens or creates the file and applies any provided functional options.

### Options

- `WithMaxBytes(maxBytes int64)`: set maximum file size (in bytes) before rotation
- `WithMaxBackups(maxBackups int)`: set the maximum number of rotated backup files
- `WithPrefix(prefix []byte)`: prepend a byte slice prefix to each log entry
- `WithFileLocking()`: enable exclusive file locking during writes and rotation

### DistributedFileWriter Methods

- `Write(b []byte) (int, error)`: buffer input until newline and then write each complete line with rotation and locking
- `Flush() error`: write any remaining buffered data as a log entry
- `Close() error`: close the underlying log file

## Contributing

Contributions and pull requests are welcome. Please run `go test ./...` to verify behavior and coverage before submitting.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
