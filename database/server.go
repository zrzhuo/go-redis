package database

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/resp/reply"
	"go-redis/utils/logger"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
)

type Server struct {
	dbs []*atomic.Value // databases
}

func MakeServer() *Server {
	server := &Server{}
	server.dbs = make([]*atomic.Value, 4) // 四个数据库
	for i := range server.dbs {
		db := MakeDatabase(i)
		holder := &atomic.Value{}
		holder.Store(db)
		server.dbs[i] = holder
	}
	return server
}

func (server *Server) Exec(redisConn _interface.Connection, cmdLine _type.CmdLine) (rep _interface.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			rep = &reply.UnknownErrReply{}
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
		return reply.MakeErrReply("selected index is invalid")
	}
	if dbIdx >= len(server.dbs) || dbIdx < 0 {
		msg := fmt.Sprintf("selected index is out of range[0, %d]", len(server.dbs)-1)
		return reply.MakeErrReply(msg)
	}
	redisConn.SetSelectDB(dbIdx)
	return reply.MakeOkReply()
}

func (server *Server) AfterConnClose(redisConn _interface.Connection) {

}

func (server *Server) Close() {

}

func (server *Server) getDB(dbIdx int) (*Database, _interface.ErrorReply) {
	if dbIdx < 0 || dbIdx >= len(server.dbs) {
		msg := fmt.Sprintf("selected index is out of range[0, %d]", len(server.dbs)-1)
		return nil, reply.MakeErrReply(msg)
	}
	return server.dbs[dbIdx].Load().(*Database), nil
}
