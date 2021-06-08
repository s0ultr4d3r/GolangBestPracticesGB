package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var dir string
var workers int

type Result struct {
	file   string
	sha256 [32]byte
}

func worker(input chan string, results chan<- *Result, wg *sync.WaitGroup) {

	log.SetFormatter(&log.JSONFormatter{})

	standartFields := log.Fields{
		"time": time.Now(),
		"func": "worker",
	}
	wlog := log.WithFields(standartFields)

	for file := range input {
		var h = sha256.New()
		var sum [32]byte
		f, err := os.Open(file)

		if err != nil {
			wlog.Errorf("open file: %v", err)
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		if _, err = io.Copy(h, f); err != nil {
			wlog.Errorf("get hash: %v", err)
			fmt.Fprintln(os.Stderr, err)
			f.Close()
			continue
		}
		f.Close()
		copy(sum[:], h.Sum(nil))
		wlog.Infof("worker result: %v   %v \n", file, sum)
		results <- &Result{
			file:   file,
			sha256: sum,
		}
	}
	wg.Done()
}

func search(input chan string) {
	log.SetFormatter(&log.JSONFormatter{})

	standartFields := log.Fields{
		"time": time.Now(),
		"func": "search",
	}
	slog := log.WithFields(standartFields)

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			slog.Errorf("walk in dir: %v", err)
			fmt.Fprintln(os.Stderr, err)
		} else if info.Mode().IsRegular() {
			input <- path
		}
		return nil
	})
	close(input)
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})

	standartFields := log.Fields{
		"time": time.Now(),
		"func": "main",
	}
	mlog := log.WithFields(standartFields)

	flag.StringVar(&dir, "dir", ".", "directory to search")
	flag.IntVar(&workers, "workers", runtime.NumCPU(), "number of workers")
	debug := flag.Bool("debug", false, "set log level to debug")
	flag.Parse()

	if *debug {
		fmt.Printf("debug level enabled \n")
		log.SetLevel(log.DebugLevel)
	}

	fmt.Printf("Searching in %s using %d workers...\n", dir, workers)

	input := make(chan string)
	results := make(chan *Result)

	mlog.Debug("cahnnels created")

	wg := sync.WaitGroup{}
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go worker(input, results, &wg)
	}

	go search(input)
	go func() {
		wg.Wait()
		close(results)
	}()

	counter := make(map[[32]byte][]string)
	for result := range results {
		counter[result.sha256] = append(counter[result.sha256], result.file)
	}

	for sha, files := range counter {
		if len(files) > 1 {
			fmt.Printf("Found %d duplicates for %s: \n", len(files), hex.EncodeToString(sha[:]))
			for _, f := range files {
				fmt.Println("-> ", f)
			}
		}
	}

}
