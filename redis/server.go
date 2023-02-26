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
	pubsub *Pubsub // pub/sub
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
	server.pubsub = MakePubsub()
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

func (server *Server) Exec(client _interface.Client, cmdLine _type.CmdLine) (reply _interface.Reply) {
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			reply = &Reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(cmdLine[0]))
	args := _type.Args(cmdLine[1:])
	// 无需auth的命令
	if cmd == "ping" {
		return server.execPing(client, args) // ping
	}
	if cmd == "auth" {
		return server.execAuth(client, args) // auth
	}
	// auth
	if !server.isAuth(client) {
		return Reply.MakeErrReply("authentication required")
	}
	// 需要auth的命令
	if cmd == "select" {
		return server.execSelect(client, args) // 选择数据库
	}
	if cmd == "subscribe" {
		return server.execSubscribe(client, args) // 订阅
	}
	if cmd == "publish" {
		return server.execPublish(client, args) // 发布
	}
	// 其他一般命令
	dbIdx := client.GetSelectDB()
	selectedDB, errReply := server.getDB(dbIdx)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Execute(client, cmdLine)
}

func (server *Server) execSubscribe(client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) < 1 {
		return Reply.MakeArgNumErrReply("subscribe")
	}
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	return server.pubsub.Subscribe(client, channels)
}

func (server *Server) execPublish(client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) != 2 {
		return Reply.MakeArgNumErrReply("publish")
	}
	channel, message := string(args[0]), args[1]
	return server.pubsub.Publish(client, channel, message)
}

func (server *Server) execPing(client _interface.Client, args _type.Args) _interface.Reply {
	size := len(args)
	if size == 0 {
		return Reply.MakePongReply()
	} else if size == 1 {
		return Reply.MakeStatusReply(string(args[0]))
	} else {
		return Reply.MakeArgNumErrReply("Ping")
	}
}

func (server *Server) execAuth(client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) != 1 {
		return Reply.MakeArgNumErrReply("auth")
	}
	if Config.RequirePass == "" {
		return Reply.MakeErrReply("no password is set.")
	}
	password := string(args[0])
	client.SetPassword(password)
	if password != Config.RequirePass {
		return Reply.MakeErrReply("invalid password.")
	}
	return Reply.MakeOkReply()
}

func (server *Server) isAuth(client _interface.Client) bool {
	if Config.RequirePass == "" {
		return true // 未设置密码
	}
	return client.GetPassword() == Config.RequirePass // 密码是否一致
}

func (server *Server) execSelect(client _interface.Client, args _type.Args) _interface.Reply {
	dbIdx, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return Reply.MakeErrReply("selected index is invalid")
	}
	if dbIdx >= len(server.databases) || dbIdx < 0 {
		msg := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.MakeErrReply(msg)
	}
	client.SetSelectDB(dbIdx) // 修改client的dbIdx
	return Reply.MakeOkReply()
}

func (server *Server) AfterClientClose(client _interface.Client) {
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
