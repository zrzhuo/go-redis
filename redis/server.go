package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/resp/reply"
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
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			rep = &Reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(cmdLine[0]))
	args := _type.Args(cmdLine[1:])
	if cmd == "ping" {
		return server.execPing(redisConn, args) // ping
	}
	if cmd == "select" {
		return server.execSelect(redisConn, cmdLine) // 选择数据库
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

func (server *Server) execPing(redisConn _interface.Connection, args _type.Args) _interface.Reply {
	size := len(args)
	if size == 0 {
		return Reply.MakePongReply()
	} else if size == 1 {
		return Reply.MakeStatusReply(string(args[0]))
	} else {
		return Reply.MakeArgNumErrReply("Ping")
	}
}

func (server *Server) execSelect(redisConn _interface.Connection, cmdLine _type.CmdLine) _interface.Reply {
	dbIdx, err := strconv.Atoi(string(cmdLine[1]))
	if err != nil {
		return Reply.MakeErrReply("selected index is invalid")
	}
	if dbIdx >= len(server.databases) || dbIdx < 0 {
		msg := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.MakeErrReply(msg)
	}
	redisConn.SetSelectDB(dbIdx) // 修改redisConn的dbIdx
	return Reply.MakeOkReply()
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
		return nil, Reply.MakeErrReply(err)
	}
	return server.databases[dbIdx].Load().(*Database), nil
}
