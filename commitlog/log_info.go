package commitlog

type LogInfo struct {
	Name      string
	MaxOffset int64
	MinOffset int64
	Retention int32
}
