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
	FsyncAlways   = "always"
	FsyncEverysec = "everysec"
	FsyncNo       = "no"
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
	pausing sync.Mutex    // 用于rewrite和fsync时暂停aof

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

	pst.msgCh = make(chan *aofMsg, 1<<16)
	pst.doneCh = make(chan struct{})
	pst.reading = false
	return pst
}

func (pst *Persister) Listening() {
	// listening
	go func() {
		for msg := range pst.msgCh {
			pst.WriteAOF(msg)
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
		pst.WriteAOF(msg)
		err := pst.file.Sync() // 直接写入
		if err != nil {
			logger.Warn(err)
		}
	} else {
		pst.msgCh <- msg // 放入aofChan，等待listening协程执行写入
	}
}

func (pst *Persister) WriteAOF(msg *aofMsg) {
	// 上锁，防止write期间进行aof重写
	pst.pausing.Lock()
	defer pst.pausing.Unlock()
	// pst针对的db与目标db不符
	if msg.dbIdx != pst.dbIdx {
		// 写入一个"Select db"命令
		cmdLine := utils.ToCmd("SELECT", []byte(strconv.Itoa(msg.dbIdx)))
		data := Reply.NewArrayReply(cmdLine).ToBytes()
		_, err := pst.file.Write(data) // 写入
		if err != nil {
			logger.Warn(err)
			return // 此时应该跳过这条命令
		}
		pst.dbIdx = msg.dbIdx // 修改pst针对的db
	}
	// 写入当前命令
	data := Reply.NewArrayReply(msg.cmdLine).ToBytes()
	_, err := pst.file.Write(data) // 写入
	if err != nil {
		logger.Warn(err)
	}
}

// ReadAOF 加载AOF文件以恢复数据，size为读取的字节数，size<0表示读取整个文件
func (pst *Persister) ReadAOF(size int64) {
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
	var reader io.Reader
	if size >= 0 {
		reader = io.LimitReader(aofFile, size) // 加载指定字节数
	} else {
		reader = aofFile // 加载整个文件
	}
	ch := resp.NewParser(reader).ParseFile()
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
		cmd, ok := payload.Data.(*Reply.ArrayReply)
		if !ok {
			logger.Error("reply error: require multi bulk reply")
			continue
		}
		// 若为"select"命令，更新dbIdx
		if strings.ToLower(string(cmd.Bulks[0])) == "select" {
			dbIdx, err := strconv.Atoi(string(cmd.Bulks[1]))
			if err == nil {
				pst.dbIdx = dbIdx
			}
		}
		// 执行命令
		reply := pst.server.ExecForAOF(aofConn, cmd.Bulks)
		if Reply.IsErrorReply(reply) {
			logger.Error("execute error: ", string(reply.ToBytes()))
		}
	}
}

func (pst *Persister) ReWrite() error {
	// 准备工作
	newFile, oldSize, oldIdx, err := pst.preReWrite()
	if err != nil {
		return err
	}
	// 创建临时server和临时persister
	tempServer := MakeFakeServer()
	tempPersister := &Persister{}
	tempPersister.filename = pst.filename
	tempPersister.server = tempServer
	tempServer.persister = tempPersister
	// 临时server加载原aof文件，只读取oldSize字节
	tempServer.persister.ReadAOF(oldSize)
	// 将临时server的数据写入新aof文件
	for i := 0; i < len(tempServer.databases); i++ {
		// select
		reply := Reply.StringToArrayReply("SELECT", strconv.Itoa(i))
		_, err = newFile.Write(reply.ToBytes())
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
			_, _ = newFile.Write(cmdLine.ToBytes())
			if expire != nil {
				expireCmd := utils.ExpireToCmd(key, expire)
				_, _ = newFile.Write(expireCmd.ToBytes())
			}
			return true
		}
		db.ForEach(operate)
	}
	// 结束工作
	pst.postReWrite(newFile, oldSize, oldIdx)
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
	newFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		logger.Warn("temp file create failed")
		return nil, 0, 0, err
	}
	return newFile, oldSize, pst.dbIdx, nil
}

func (pst *Persister) postReWrite(newFile *os.File, oldSize int64, oldIdx int) {
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
	// Seek到文件的指定位置
	_, err = oldFile.Seek(oldSize, 0)
	if err != nil {
		logger.Error("seek AOF file failed: " + err.Error())
		return
	}
	// select之前的db
	reply := Reply.StringToArrayReply("SELECT", strconv.Itoa(oldIdx))
	_, err = newFile.Write(reply.ToBytes())
	if err != nil {
		logger.Error("rewrite AOF file failed: " + err.Error())
		return
	}
	// Copy新命令
	_, err = io.Copy(newFile, oldFile)
	if err != nil {
		logger.Error("copy AOF file failed: " + err.Error())
		return
	}
	// select现在的db
	reply = Reply.StringToArrayReply("SELECT", strconv.Itoa(pst.dbIdx))
	_, err = newFile.Write(reply.ToBytes())
	if err != nil {
		logger.Error("rewrite AOF file failed: " + err.Error())
		return
	}
	// 替换新aof文件
	err = pst.file.Close() // 关闭原来的file
	if err != nil {
		logger.Error("rewrite AOF file failed: " + err.Error())
		panic(err)
	}
	err = os.Rename(newFile.Name(), pst.filename)
	if err != nil {
		logger.Error("rewrite AOF file failed: " + err.Error())
		panic(err)
	}
	file, err := os.OpenFile(pst.filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600) // 重新打开
	if err != nil {
		logger.Error("rewrite AOF file failed: " + err.Error())
		panic(err)
	}
	pst.file = file
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
