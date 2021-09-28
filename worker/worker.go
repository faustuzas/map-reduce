package worker

import (
	"log"

	"github.com/faustuzas/map-reduce/types"
)

type worker struct {
	mapTasks    <-chan types.MapTask
	reduceTasks <-chan types.ReduceTask
	done        <-chan struct{}
}

func NewWorker(mapTasks <-chan types.MapTask, reduceTasks <-chan types.ReduceTask, done <-chan struct{}) *worker {
	return &worker{
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
		done:        done,
	}
}

func (w *worker) Run() {
	for {
		select {
		case mapTask := <-w.mapTasks:
			log.Printf("map task received: %v", mapTask)
		case reduceTask := <-w.reduceTasks:
			log.Printf("reduce task received: %v", reduceTask)
		case <-w.done:
			log.Println("worker shutting down...")
			return
		}
	}
}
