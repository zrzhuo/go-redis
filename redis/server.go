package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	reply2 "go-redis/resp/reply"
	"go-redis/utils/logger"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
)

const NumOfDatabases = 4

type Server struct {
	databases []*atomic.Value // 若干个redis数据库
	//persister *Persister      // AOF持久化
}

func MakeServer() *Server {
	server := &Server{}
	server.databases = make([]*atomic.Value, NumOfDatabases) // 四个数据库
	for i := range server.databases {
		db := MakeDatabase(i)
		holder := &atomic.Value{}
		holder.Store(db)
		server.databases[i] = holder
	}
	// AOF持久化
	if Config.AppendOnly {
		filename, fsync := Config.AppendFilename, Config.AppendFsync
		persister, err := NewPersister(server, filename, fsync)
		if err != nil {
			panic(err)
		}
		// 为每个database开启aof
		for i := range server.databases {
			db := server.databases[i].Load().(*Database)
			db.ToAof = func(cmdLine _type.CmdLine) {
				persister.ToAOF(db.idx, cmdLine)
			}
		}
		persister.ReadAof()   // 加载aof文件
		persister.listening() // 开启aof监听
	}
	return server
}

func (server *Server) Exec(redisConn _interface.Connection, cmdLine _type.CmdLine) (rep _interface.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			rep = &reply2.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(cmdLine[0]))
	if cmd == "select" {
		// 选择数据库
		return server.execSelect(redisConn, cmdLine)
	} else {
		// 一般命令
		dbIdx := redisConn.GetSelectDB()
		selectedDB, errReply := server.getDB(dbIdx)
		if errReply != nil {
			return errReply
		}
		return selectedDB.Execute(redisConn, cmdLine)
	}
}

func (server *Server) execSelect(redisConn _interface.Connection, cmdLine _type.CmdLine) _interface.Reply {
	dbIdx, err := strconv.Atoi(string(cmdLine[1]))
	if err != nil {
		return reply2.MakeErrReply("selected index is invalid")
	}
	if dbIdx >= len(server.databases) || dbIdx < 0 {
		msg := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return reply2.MakeErrReply(msg)
	}
	redisConn.SetSelectDB(dbIdx) // 修改redisConn的dbIdx
	return reply2.MakeOkReply()
}

func (server *Server) AfterConnClose(redisConn _interface.Connection) {
	logger.Info("connection is closed, do something...")
}

func (server *Server) Close() {
	logger.Info("redis server is closing...")
}

func (server *Server) getDB(dbIdx int) (*Database, _interface.ErrorReply) {
	if dbIdx < 0 || dbIdx >= len(server.databases) {
		err := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return nil, reply2.MakeErrReply(err)
	}
	return server.databases[dbIdx].Load().(*Database), nil
}
