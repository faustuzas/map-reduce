package worker

import (
	"bufio"
	"bytes"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/faustuzas/map-reduce/types"
	"github.com/faustuzas/map-reduce/util"
)

const (
	_reduceOutputDir = "output/reduce"
)

func (w *Worker) handleReduceTask(task types.ReduceTask) {
	// TODO: can be optimized to use merge sort
	grouped := map[string][]string{}

	for _, path := range task.FilePaths {
		inputFile, err := os.Open(path)
		defer util.CloseWithErrLog(inputFile.Close)

		if err != nil {
			task.Result <- types.TaskResult{
				Err: fmt.Errorf("[ERROR] opening the file %s: %v", path, err),
			}
			return
		}

		scanner := bufio.NewScanner(inputFile)
		var key, value, entry string

		for scanner.Scan() {
			entry = scanner.Text()

			n, err := fmt.Sscanf(entry, "%s\t%s", &key, &value)
			if err != nil {
				task.Result <- types.TaskResult{
					Err: fmt.Errorf("[ERROR] parsing reduce entry: %w", err),
				}
				return
			}

			if n != 2 {
				task.Result <- types.TaskResult{
					Err: fmt.Errorf("[ERROR] not all arguments were parsed from entry %q", entry),
				}
				return
			}

			grouped[key] = append(grouped[key], value)
		}
	}

	var buff []types.KeyValue
	emit := types.EmitFunc(func(kv types.KeyValue) {
		buff = append(buff, kv)
	})

	for key, values := range grouped {
		kvs := types.KeyValues{
			Key:    key,
			Values: values,
		}

		if err := task.F(kvs, emit); err != nil {
			task.Result <- types.TaskResult{
				Err: fmt.Errorf("[ERROR] reduce func failed on entry %v: %w", kvs, err),
			}
			return
		}
	}

	var buf bytes.Buffer
	for _, kv := range buff {
		fmt.Fprintf(&buf, "%s\t%s\n", kv.Key, kv.Value)
	}

	err := os.MkdirAll(_reduceOutputDir, fs.ModePerm)
	if err != nil {
		task.Result <- types.TaskResult{
			Err: fmt.Errorf("[ERROR] creating directories %v: %w", _reduceOutputDir, err),
		}
		return
	}

	outputFile := path.Join(_reduceOutputDir, task.TaskId)
	err = ioutil.WriteFile(outputFile, buf.Bytes(), os.ModePerm)
	if err != nil {
		task.Result <- types.TaskResult{
			Err: fmt.Errorf("[ERROR] writing reduce result to the file %v: %w", outputFile, err),
		}
		return
	}

	task.Result <- types.TaskResult{
		ResultPaths: []string{outputFile},
	}

	log.Printf("Reduce task %s is finished successfully", task.TaskId)
}
