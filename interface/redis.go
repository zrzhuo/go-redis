package _interface

// Reply is the interface of redis serialization resp message
type Reply interface {
	ToBytes() []byte
}

// ErrorReply is an error and redis.Reply
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// Connection represents a connection with redis client
type Connection interface {
	Write([]byte) (int, error)
	Close() error
	RemoteAddr() string

	GetSelectDB() int
	SetSelectDB(int)

	SetPassword(string)
	GetPassword() string

	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string

	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32
	AddTxError(err error)
	GetTxErrors() []error

	SetSlave()
	IsSlave() bool
	SetMaster()
	IsMaster() bool
}
