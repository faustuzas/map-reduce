package worker

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/faustuzas/map-reduce/types"
	"github.com/faustuzas/map-reduce/util"
)

const (
	_mapOutputDir = "output/map"
)

func (w *Worker) handleMapTask(task types.MapTask) {
	inputFile, err := os.Open(task.FilePath)
	defer util.CloseWithErrLog(inputFile.Close)

	if err != nil {
		task.Result <- types.TaskResult{
			Err: fmt.Errorf("[ERROR] opening the file %s: %v", task.FilePath, err),
		}
		return
	}

	var buff []types.KeyValue
	emit := types.EmitFunc(func(kv types.KeyValue) {
		buff = append(buff, kv)
	})

	scanner := bufio.NewScanner(inputFile)
	lineNumber := 1
	for scanner.Scan() {
		payload := types.KeyValue{
			Key:   strconv.Itoa(lineNumber),
			Value: scanner.Text(),
		}

		task.F(payload, emit)

		lineNumber++
	}

	if err := scanner.Err(); err != nil {
		task.Result <- types.TaskResult{
			Err: fmt.Errorf("[ERROR] scanning file %s: %v", task.FilePath, err),
		}
		return
	}

	partitioned := w.partitionMapOutput(buff)
	for _, partition := range partitioned {
		sort.Slice(partition, func(i, j int) bool {
			return partition[i].Key < partition[j].Key
		})
	}

	var buf bytes.Buffer

	basePath := path.Join(_mapOutputDir, task.TaskId)
	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		task.Result <- types.TaskResult{
			Err: fmt.Errorf("[ERROR] creating directories for path %s: %v", basePath, err),
		}
		return
	}

	var result []string
	for i, partiton := range partitioned {
		buf.Reset()

		for _, kv := range partiton {
			fmt.Fprintf(&buf, "%q\t%q\n", kv.Key, kv.Value)
		}

		partitionPath := path.Join(basePath, strconv.Itoa(i))
		err := ioutil.WriteFile(partitionPath, buf.Bytes(), os.ModePerm)
		if err != nil {
			task.Result <- types.TaskResult{
				Err: fmt.Errorf("[ERROR] writing data to partition file %s: %v", partitionPath, err),
			}
			return
		}

		result = append(result, partitionPath)
	}

	task.Result <- types.TaskResult{
		ResultPaths: result,
	}

	log.Printf("Task %s is finished successfully", task.TaskId)
}

func (w *Worker) partitionMapOutput(output []types.KeyValue) [][]types.KeyValue {
	result := make([][]types.KeyValue, w.ReducerCount)

	for _, kv := range output {
		partitionIdx := hash(kv.Key) % w.ReducerCount
		result[partitionIdx] = append(result[partitionIdx], kv)
	}

	return result
}

func hash(value string) int {
	h := fnv.New32a()
	h.Write([]byte(value))
	return int(h.Sum32())
}
