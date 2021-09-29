package worker

import (
	"log"

	"github.com/faustuzas/map-reduce/types"
)

type Worker struct {
	MapTasks    <-chan types.MapTask
	ReduceTasks <-chan types.ReduceTask
	Done        <-chan struct{}

	ReducerCount int
}

func (w *Worker) Run() {
	for {
		select {
		case mapTask := <-w.MapTasks:
			w.handleMapTask(mapTask)
		case reduceTask := <-w.ReduceTasks:
			w.handleReduceTask(reduceTask)
		case <-w.Done:
			log.Println("worker shutting down...")
			return
		}
	}
}
