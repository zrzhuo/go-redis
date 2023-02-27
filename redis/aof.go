package redis

import (
	"context"
	_type "go-redis/interface/type"
	"go-redis/redis/utils"
	"go-redis/resp"
	Reply "go-redis/resp/reply"
	"go-redis/utils/logger"
	"io"
	"io/ioutil"
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
	server *Server // 当前针对的服务实例
	dbIdx  int     // 当前针对的server中的数据库

	filename string   // aof文件路径
	fsync    string   // aof文件写入策略：always/everysec/no
	file     *os.File // aof文件描述符

	msgCh   chan *aofMsg  // 主线程通知Persister进行aof
	doneCh  chan struct{} // 通知主线程aof操作已完成
	reading bool          // 是否正处于reading状态
	pausing sync.Mutex    // 用于暂停aof

}

func NewPersister(server *Server, filename string, fsync string) *Persister {
	var pst = &Persister{}
	ctx, cancel := context.WithCancel(context.Background())
	pst.ctx = ctx
	pst.cancel = cancel
	pst.server = server
	pst.dbIdx = 0

	pst.filename = filename
	pst.fsync = fsync
	aofFile, err := os.OpenFile(pst.filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	pst.file = aofFile

	pst.reading = false
	pst.msgCh = make(chan *aofMsg, aofChanSize)
	pst.doneCh = make(chan struct{})

	return pst
}

func (pst *Persister) listening() {
	// listening
	go func() {
		for msg := range pst.msgCh {
			pst.writeAof(msg)
		}
		pst.doneCh <- struct{}{}
	}()
	// everysec
	if pst.fsync == FsyncEverysec {
		ticker := time.NewTicker(time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					pst.pausing.Lock() // 暂停aof
					err := pst.file.Sync()
					if err != nil {
						logger.Error("fsync failed: " + err.Error())
					}
					pst.pausing.Unlock()
				case <-pst.ctx.Done():
					return
				}
			}
		}()
	}
}

func (pst *Persister) ToAOF(dbIdx int, cmdLine _type.CmdLine) {
	// reading状态中不进行写入
	if pst.reading {
		return
	}
	msg := &aofMsg{
		cmdLine: cmdLine,
		dbIdx:   dbIdx,
	}
	// always
	if pst.fsync == FsyncAlways {
		pst.writeAof(msg) // 直接写入
	} else {
		pst.msgCh <- msg // 放入aofChan，等待listening协程执行写入
	}
}

func (pst *Persister) writeAof(p *aofMsg) {
	// 上锁，防止write期间进行aof重写
	pst.pausing.Lock()
	defer pst.pausing.Unlock()
	// pst针对的db与目标db不符
	if p.dbIdx != pst.dbIdx {
		// 写入一个"Select db"命令
		cmdLine := utils.ToCmd("SELECT", []byte(strconv.Itoa(p.dbIdx)))
		data := Reply.MakeArrayReply(cmdLine).ToBytes()
		_, err := pst.file.Write(data) // 写入
		if err != nil {
			logger.Warn(err)
			return // 此时应该跳过这条命令
		}
		pst.dbIdx = p.dbIdx // 修改pst针对的db
	}
	// 写入当前命令
	data := Reply.MakeArrayReply(p.cmdLine).ToBytes()
	_, err := pst.file.Write(data) // 写入
	if err != nil {
		logger.Warn(err)
	}
	// always
	if pst.fsync == FsyncAlways {
		err = pst.file.Sync()
		if err != nil {
			logger.Warn(err)
		}
	}
}

func (pst *Persister) ReadAof() {
	// 开启reading状态，防止read过程中的命令重新写入aof文件
	pst.reading = true
	defer func() {
		pst.reading = false
	}()
	// 打开文件
	aofFile, err := os.Open(pst.filename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer aofFile.Close()
	// 解析文件
	ch := resp.MakeParser(aofFile).ParseFile()
	aofConn := GetAofClient()
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
		reply, ok := payload.Data.(*Reply.ArrayReply)
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

func (pst *Persister) ReadAofWithSize(size int64) {
	// 开启reading状态，防止read过程中的命令重新写入aof文件
	pst.reading = true
	defer func() {
		pst.reading = false
	}()
	// 打开文件
	aofFile, err := os.Open(pst.filename)
	if err != nil {
		logger.Warn(err)
		return
	}
	defer aofFile.Close()
	// 解析文件，只读取size字节
	reader := io.LimitReader(aofFile, size)
	ch := resp.MakeParser(reader).ParseFile()
	aofConn := GetAofClient()
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
		reply, ok := payload.Data.(*Reply.ArrayReply)
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
	if pst.file != nil {
		close(pst.msgCh)
		<-pst.doneCh
		err := pst.file.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	pst.cancel()
}

func (pst *Persister) ReWrite() error {
	tempFile, fileSize, dbIdx, err := pst.preReWrite()
	if err != nil {
		return err
	}
	// 创建临时server和临时persister
	tempServer := MakeTempServer()
	tempPersister := &Persister{}
	tempPersister.filename = pst.filename
	tempPersister.server = tempServer
	tempServer.persister = tempPersister
	// 临时server加载原aof文件，只读取fileSize字节
	tempServer.persister.ReadAofWithSize(fileSize)
	// 将临时server的数据写入新aof文件
	for i := 0; i < len(tempServer.databases); i++ {
		// select
		reply := Reply.ToArrayReply("SELECT", strconv.Itoa(i))
		_, err = tempFile.Write(reply.ToBytes())
		if err != nil {
			return err
		}
		// 写入命令
		db := tempServer.databases[i].Load().(*Database)
		operate := func(key string, entity *_type.Entity, expire *time.Time) bool {
			if entity == nil {
				return true
			}
			cmdLine := utils.EntityToCmd(key, entity)
			_, _ = tempFile.Write(cmdLine.ToBytes())
			if expire != nil {
				expireCmd := utils.ExpireToCmd(key, expire)
				_, _ = tempFile.Write(expireCmd.ToBytes())
			}
			return true
		}
		db.ForEach(operate)
	}
	pst.postReWrite(tempFile, fileSize, dbIdx)
	// 关闭资源
	tempServer.Close()
	tempPersister.Close()
	return nil
}

func (pst *Persister) preReWrite() (*os.File, int64, int, error) {
	// 上锁
	pst.pausing.Lock()
	defer pst.pausing.Unlock()
	// 写磁盘
	err := pst.file.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, 0, 0, err
	}
	// 记录当前aof文件大小
	fileInfo, err := os.Stat(pst.filename)
	if err != nil {
		logger.Warn("wrong aof file path")
		return nil, 0, 0, err
	}
	oldSize := fileInfo.Size()
	//创建新aof文件
	tempFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		logger.Warn("temp file create failed")
		return nil, 0, 0, err
	}
	return tempFile, oldSize, pst.dbIdx, nil
}

func (pst *Persister) postReWrite(newFile *os.File, oldSize int64, dbIdx int) {
	// 上锁
	pst.pausing.Lock()
	defer pst.pausing.Unlock()
	// 将旧aof文件中的新命令写入到新aof文件中
	oldFile, err := os.Open(pst.filename)
	if err != nil {
		logger.Error("open AOF file failed: " + err.Error())
		return
	}
	defer func() {
		_ = oldFile.Close()
	}()
	// Seek
	_, err = oldFile.Seek(oldSize, 0)
	if err != nil {
		logger.Error("seek AOF file failed: " + err.Error())
		return
	}

	//sync tmpFile's db index with online file
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}

	// Copy
	_, err = io.Copy(newFile, oldFile)
	if err != nil {
		logger.Error("copy AOF file failed: " + err.Error())
		return
	}
	// 用新aof文件进行替换
	_ = pst.file.Close() // 关闭原来的file
	_ = os.Rename(newFile.Name(), pst.filename)
	file, err := os.OpenFile(pst.filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600) // 重新打开
	if err != nil {
		panic(err)
	}
	pst.file = file

	//write select command again to ensure aof file has the same db index with  persister.currentDB
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
