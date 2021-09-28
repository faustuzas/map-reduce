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

type MapTask struct {
	FilePath string
	F        MapFunc
}

type ReduceTask struct {
	FilePath string
	F        ReduceFunc
}
