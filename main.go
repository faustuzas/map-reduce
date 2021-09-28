package main

import (
	"flag"
	"io/fs"
	"log"
	"path/filepath"

	"github.com/faustuzas/map-reduce/types"
	"github.com/faustuzas/map-reduce/worker"
)

func main() {
	var (
		workerCount = flag.Int("worker-count", 1, "number of concurrent workers")
		inputDir    = flag.String("input-dir", "test_data", "input directory of files to process")
	)
	flag.Parse()

	if *inputDir == "" {
		log.Fatal("input-dir cannot be empty")
	}

	var (
		done        = make(chan struct{})
		mapTasks    = make(chan types.MapTask)
		reduceTasks = make(chan types.ReduceTask)
	)

	for i := 0; i < *workerCount; i++ {
		go worker.NewWorker(mapTasks, reduceTasks, done).Run()
	}

	filesWalked := 0
	filepath.WalkDir(*inputDir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		mapTasks <- types.MapTask{
			FilePath: path,
			F:        types.MapFunc(testMapFunc),
		}

		filesWalked++
		return nil
	})

	if filesWalked == 0 {
		log.Fatal("empty input-dir provided")
	}

	close(done)
}

func testMapFunc(kv types.KeyValue, emit types.EmitFunc) error {
	log.Printf("map func: %v", kv)
	return nil
}
