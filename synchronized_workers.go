package main

import (
	"bufio"
	"context"
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kolide/kit/env"
	"github.com/kolide/kit/logutil"
	"github.com/pkg/errors"
)

type workerManager struct {
	logger  log.Logger
	jobs    <-chan string
	results chan<- error

	outFile     *os.File
	outFileLock sync.Mutex
}

func (w *workerManager) spawn(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case input, ok := <-w.jobs:
			if !ok {
				return
			}
			w.results <- w.work(input)
		}
	}
}

func (w *workerManager) work(input string) error {
	w.outFileLock.Lock()
	defer w.outFileLock.Unlock()
	if _, err := w.outFile.Write([]byte(input + "\n")); err != nil {
		return errors.Wrap(err, "writing to out file")
	}
	return nil
}

func main() {
	var (
		flDebug      = flag.Bool("debug", env.Bool("DEBUG", true), "Whether or not to enable debug logging")
		flDataDir    = flag.String("data-dir", env.String("DATA_DIR", "./data/names"), "The directory of data files")
		flOutFile    = flag.String("out-file", env.String("OUT_FILE", "./output.csv"), "The path to output a csv")
		flNumWorkers = flag.Int("num-workers", env.Int("NUM_WORKERS", 100), "The number of goroutines in the pool")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
	logger := logutil.NewServerLogger(*flDebug)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan string, *flNumWorkers)
	results := make(chan error, *flNumWorkers)
	outFile, err := os.Create(*flOutFile)
	if err != nil {
		logutil.Fatal(logger, "msg", "creating out file", "err", err)
	}
	defer outFile.Close()
	workers := &workerManager{
		logger:  logger,
		jobs:    jobs,
		results: results,
		outFile: outFile,
	}
	for i := 0; i < *flNumWorkers; i++ {
		go workers.spawn(ctx)
	}
	level.Debug(logger).Log("msg", "spawned workers", "count", *flNumWorkers)

	files, err := ioutil.ReadDir(*flDataDir)
	if err != nil {
		logutil.Fatal(logger, "msg", "error reading data directory", "err", err)
	}

	level.Debug(logger).Log("msg", "collected data files", "count", len(files))

	jobCount := 0
	for _, fileInfo := range files {
		path := filepath.Join(*flDataDir, fileInfo.Name())
		level.Debug(logger).Log("msg", "launching jobs for lines in file", "path", path)

		file, err := os.Open(path)
		if err != nil {
			logutil.Fatal(logger, "msg", "couldn't open data file", "path", path, "err", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			jobCount++
			jobs <- scanner.Text()
		}

		if err := scanner.Err(); err != nil {
			logutil.Fatal(logger, "msg", "error reading file", "path", path, "err", err)
		}
		level.Debug(logger).Log("msg", "finished launching jobs for lines in file", "path", path)
	}
	close(jobs)
	level.Debug(logger).Log("msg", "launched all jobs", "count", jobCount)

	for i := 0; i < jobCount; i++ {
		if err := <-results; err != nil {
			level.Error(logger).Log("msg", "error running job", "err", err)
		}
	}

	level.Info(logger).Log("msg", "finished running all jobs", "count", jobCount)
}
