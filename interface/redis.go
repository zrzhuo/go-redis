package _interface

import _type "go-redis/interface/type"

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
	InitWatch(dbNum int)
	DestoryWatch()
	SetWatchKey(dbIdx int, key string, version int)
	GetWatchKeys() []map[string]int
}

type Server interface {
	ExecWithLock(client Client, cmdLine _type.CmdLine) Reply
	ExecWithoutLock(client Client, cmdLine _type.CmdLine) Reply

	SetTxing(flag bool)
	IsTxing() bool

	CloseClient(client Client)
	Close()
}
