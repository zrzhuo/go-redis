package _interface

import _type "go-redis/interface/type"

type Reply interface {
	ToBytes() []byte
}

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type Client interface {
	Write([]byte) (int, error)
	Close() error
	RemoteAddr() string

	GetSelectDB() int
	SetSelectDB(int)

	SetPassword(string)
	GetPassword() string

	Subscribe(channel string)
	UnSubscribe(channel string)
	ChannelsCount() int
	GetChannels() []string

	IsTxState() bool
	SetTxState(flag bool)
	EnTxQueue(cmdLine _type.CmdLine)
	GetTxQueue() []_type.CmdLine
	ClearTxQueue()
	AddTxError(err error)
	GetTxError() []error

	//GetWatching() map[string]uint32
}

type Server interface {
	ExecWithLock(client Client, cmdLine _type.CmdLine) Reply
	CloseClient(client Client)
	Close()
}
