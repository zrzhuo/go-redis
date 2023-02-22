package redis

import (
	"context"
	_type "go-redis/interface/type"
	"go-redis/redis/utils"
	"go-redis/resp"
	Reply "go-redis/resp/reply"
	"go-redis/utils/logger"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	aofChanSize = 1 << 16

	FsyncAlways   = "always"   // do fsync for every command
	FsyncEverysec = "everysec" // do fsync every second
	FsyncNo       = "no"       // lets operating system decides when to do fsync
)

type aofMsg struct {
	cmdLine _type.CmdLine
	dbIdx   int
	wg      *sync.WaitGroup
}

type Persister struct {
	ctx    context.Context
	cancel context.CancelFunc
	server *Server
	dbIdx  int // 当前针对的数据库

	aofFilePath string   // aof文件路径
	aofFsync    string   // aof文件写入策略：always/everysec/no
	aofFile     *os.File // aof文件描述符

	aofMsgCh  chan *aofMsg  // 主线程通知Persister进行aof
	aofDoneCh chan struct{} // 通知主线程aof操作已完成
	closed    bool          // aofChan是否被暂时关闭
}

// NewPersister creates a new aof.Persister
func NewPersister(server *Server, filename string, fsync string, load bool) (*Persister, error) {
	var pst = &Persister{}
	ctx, cancel := context.WithCancel(context.Background())
	pst.ctx = ctx
	pst.cancel = cancel
	pst.server = server
	pst.dbIdx = 0

	pst.aofFilePath = filename
	pst.aofFsync = fsync
	aofFile, err := os.OpenFile(pst.aofFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	pst.aofFile = aofFile

	pst.aofMsgCh = make(chan *aofMsg, aofChanSize)
	pst.aofDoneCh = make(chan struct{})
	pst.closed = false

	//// 加载aof文件
	//if load {
	//	pst.readAof()
	//}

	// 监听aofMsgCh，写入aof文件
	go func() {
		pst.listening()
	}()

	// 每秒做一次持久化
	if pst.aofFsync == FsyncEverysec {
		pst.fsyncEverySecond()
	}
	return pst, nil
}

func (pst *Persister) listening() {
	for msg := range pst.aofMsgCh {
		pst.writeAof(msg)
	}
	pst.aofDoneCh <- struct{}{}
}

func (pst *Persister) ToAOF(dbIdx int, cmdLine _type.CmdLine) {
	// 检查aofChan是否被关闭
	if pst.closed {
		return
	}
	if pst.aofFsync == FsyncAlways {
		// 直接写入
		p := &aofMsg{
			cmdLine: cmdLine,
			dbIdx:   dbIdx,
		}
		pst.writeAof(p)
		return
	}
	// 放入aofChan，等待aof协程执行写入
	pst.aofMsgCh <- &aofMsg{
		cmdLine: cmdLine,
		dbIdx:   dbIdx,
	}
}

func (pst *Persister) writeAof(p *aofMsg) {
	// pst针对的db与目标db不符
	if p.dbIdx != pst.dbIdx {
		// 写入一个"Select db"命令
		cmdLine := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIdx))
		data := Reply.MakeMultiBulkReply(cmdLine).ToBytes()
		_, err := pst.aofFile.Write(data) // 写入
		if err != nil {
			logger.Warn(err)
			return // 此时应该跳过这条命令
		}
		pst.dbIdx = p.dbIdx // 修改pst针对的db
	}
	// 写入当前命令
	data := Reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
	_, err := pst.aofFile.Write(data) // 写入
	if err != nil {
		logger.Warn(err)
	}
	if pst.aofFsync == FsyncAlways {
		_ = pst.aofFile.Sync()
	}
}

func (pst *Persister) readAof() {
	// 将aof通道暂时关闭
	pst.closeChan()
	defer func() {
		pst.openChan()
	}()

	file, err := os.Open(pst.aofFilePath)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	ch := resp.MakeParser(file).ParseStream()
	aofConn := GetAofConn()
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break // 已结束
			}
			logger.Error("parse error: " + payload.Err.Error())
			continue
		}
		if payload.Data == nil {
			logger.Error("reply error: reply is nil")
			continue
		}
		reply, ok := payload.Data.(*Reply.MultiBulkReply)
		if !ok {
			logger.Error("type error: require multi bulk reply")
			continue
		}
		// 若为"select"命令，更新dbIdx
		if strings.ToLower(string(reply.Args[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(reply.Args[1]))
			if err == nil {
				pst.dbIdx = dbIndex
			}
		}
		// 执行命令
		res := pst.server.Exec(aofConn, reply.Args)
		if Reply.IsErrorReply(reply) {
			logger.Error("execute error: ", string(res.ToBytes()))
		}

	}
}

func (pst *Persister) Close() {
	if pst.aofFile != nil {
		close(pst.aofMsgCh)
		<-pst.aofDoneCh
		err := pst.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	pst.cancel()
}

func (pst *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				//pst.pausingAof.Lock()
				if err := pst.aofFile.Sync(); err != nil {
					logger.Error("fsync failed: " + err.Error())
				}
				//pst.pausingAof.Unlock()
			case <-pst.ctx.Done():
				return
			}
		}
	}()
}

func (pst *Persister) openChan() {
	pst.closed = false
}

func (pst *Persister) closeChan() {
	pst.closed = true
}
