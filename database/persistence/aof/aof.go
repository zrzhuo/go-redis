package aof

import (
	_type "go-redis/interface/type"
	"os"
	"sync"
)

type payload struct {
	cmdLine _type.CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

type Persister struct {
	aofChan     chan *payload // 主线程使用此channel将要持久化的命令发送到异步协程
	aofFile     *os.File      // aof文件描述符
	aofFilename string        // aof文件路径
	aofFsync    string
}
