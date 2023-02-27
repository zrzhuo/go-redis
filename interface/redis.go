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
	SubsCount() int
	GetChannels() []string
}

type DB interface {
	Exec(client Client, cmdLine _type.CmdLine) Reply
	CloseClient(client Client)
	Close()
}
