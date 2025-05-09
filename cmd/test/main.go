package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/romosch/dfwriter"
)

func main() {
	log := flag.String("log", "", "log file")
	prefix := flag.String("prefix", "", "id")
	lines := flag.Int("lines", 0, "number of lines to write")
	lineSize := flag.Int("lineSize", 0, "number of lines to write")
	rot := flag.Int64("rotationSize", 0, "rotation size")
	lock := flag.Bool("lock", false, "use file locking")

	flag.Parse()

	options := []dfwriter.Option{
		dfwriter.WithMaxBytes(*rot),
		dfwriter.WithMaxBackups(10000),
		dfwriter.WithPrefix([]byte(*prefix)),
	}
	if *lock {
		options = append(options, dfwriter.WithFileLocking())
	}

	logger, err := dfwriter.New(*log, options...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "id=%s: New: %v\n", *prefix, err)
		os.Exit(1)
	}
	defer logger.Close()

	msg := []byte(strings.Repeat("x", *lineSize-len(*prefix)-1) + "\n")
	for j := 0; j < *lines; j++ {
		if _, err := logger.Write(msg); err != nil {
			fmt.Fprintf(os.Stderr, "id=%s write: %v\n", *prefix, err)
			os.Exit(1)
		}
	}
	if err := logger.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "id=%s sync: %v\n", *prefix, err)
		os.Exit(1)
	}
}
