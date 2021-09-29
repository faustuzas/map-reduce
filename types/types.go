package types

type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	Key    string
	Values []string
}

type EmitFunc func(kv KeyValue)
type MapFunc func(kv KeyValue, emit EmitFunc) error
type ReduceFunc func(kvs KeyValues, emit EmitFunc) error

type TaskResult struct {
	ResultPaths []string
	Err         error
}

type MapTask struct {
	TaskId   string
	FilePath string
	F        MapFunc

	Result chan<- TaskResult
}

type ReduceTask struct {
	TaskId   string
	FilePath string
	F        ReduceFunc

	Result chan<- TaskResult
}
