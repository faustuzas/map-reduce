package main

import (
	"flag"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/faustuzas/map-reduce/types"
	"github.com/faustuzas/map-reduce/worker"
	"github.com/google/uuid"
)

func main() {
	var (
		workerCount  = flag.Int("worker-count", 1, "number of concurrent workers")
		reducerCount = flag.Int("reducer-count", 1, "number or reducers")
		inputDir     = flag.String("input-dir", "test_data", "input directory of files to process")
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
		worker := worker.Worker{
			MapTasks:     mapTasks,
			ReduceTasks:  reduceTasks,
			Done:         done,
			ReducerCount: *reducerCount,
		}
		go worker.Run()
	}

	var inputFilePaths []string
	filepath.WalkDir(*inputDir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		inputFilePaths = append(inputFilePaths, path)
		return nil
	})

	if len(inputFilePaths) == 0 {
		log.Fatal("empty input-dir provided")
	}

	mapResultCh := make(chan types.TaskResult, len(inputFilePaths))
	for _, path := range inputFilePaths {
		go func(path string) {
			task := types.MapTask{
				TaskId:   uuid.NewString(),
				FilePath: path,
				F:        types.MapFunc(testMapFunc),
				Result:   mapResultCh,
			}

			log.Printf("[MASTER] submitting map task %s for path %s", task.TaskId, task.FilePath)

			mapTasks <- task
		}(path)
	}

	var results [][]string
	for i := 0; i < len(inputFilePaths); i++ {
		result := <-mapResultCh

		if result.Err != nil {
			log.Printf("[MASTER] error in mapping task: %v", result.Err)
			return
		}

		results = append(results, result.ResultPaths)
	}

	fmt.Printf("result: %v", results)

	close(done)
	time.Sleep(2 * time.Second)
}

func testMapFunc(kv types.KeyValue, emit types.EmitFunc) error {
	for _, w := range strings.Split(kv.Value, " ") {
		emit(types.KeyValue{
			Key:   w,
			Value: "1",
		})
	}

	return nil
}
