package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/resp/reply"
	"go-redis/utils/logger"
	"runtime/debug"
	"strings"
	"sync/atomic"
)

type Server struct {
	databases []*atomic.Value // 若干个redis数据库
	persister *Persister      // AOF持久化
	pubsub    *Pubsub         // pub/sub
}

// MakeServer 读取配置，创建server
func MakeServer() *Server {
	server := &Server{}
	// 创建指定个数的db，默认为16
	dbNum := 16
	if Config.Databases > 0 {
		dbNum = Config.Databases
	}
	server.databases = make([]*atomic.Value, dbNum)
	for i := range server.databases {
		db := MakeDatabase(i)
		holder := &atomic.Value{}
		holder.Store(db)
		server.databases[i] = holder
	}
	// pub/sub
	server.pubsub = MakePubsub()
	// AOF持久化
	if Config.AppendOnly {
		filename, fsync := Config.AppendFilename, Config.AppendFsync
		persister := NewPersister(server, filename, fsync)
		// 为每个database开启aof
		for i := range server.databases {
			db := server.databases[i].Load().(*Database)
			db.ToAOF = func(cmdLine _type.CmdLine) {
				persister.ToAOF(db.idx, cmdLine)
			}
		}
		persister.ReadAOF(-1) // 加载整个AOF文件
		persister.Listening() // 开启AOF监听
		server.persister = persister
	}
	return server
}

// MakeTempServer 创建一个临时server，用于AOF重写
func MakeTempServer() *Server {
	server := &Server{}
	// 创建指定个数的db，默认为16
	dbNum := 16
	if Config.Databases > 0 {
		dbNum = Config.Databases
	}
	server.databases = make([]*atomic.Value, dbNum)
	for i := range server.databases {
		db := MakeSimpleDatabase(i)
		holder := &atomic.Value{}
		holder.Store(db)
		server.databases[i] = holder
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
	// 解析命令行
	cmd := strings.ToLower(string(cmdLine[0]))
	args := _type.Args(cmdLine[1:])
	// 无需auth的命令
	if cmd == "ping" {
		return server.execPing(client, args) // ping
	}
	if cmd == "auth" {
		return server.execAuth(client, args) // auth
	}
	// 判断auth
	if !server.isAuth(client) {
		return Reply.MakeErrReply("authentication required")
	}
	// 需要auth的命令
	switch cmd {
	case "select":
		return server.execSelect(client, args) // 选择数据库
	case "flushdb":
		return server.execFlushDB(client, args) // 清空数据库
	case "flushall":
		return server.execFlushAll(client, args) // 清空所有数据库
	case "subscribe":
		return server.execSubscribe(client, args) // 订阅
	case "unsubscribe":
		return server.execUnSubscribe(client, args) // 订阅
	case "publish":
		return server.execPublish(client, args) // 发布
	case "rewriteaof":
		return server.execReWriteAOF(client, args) // aof重写
	case "bgrewriteaof":
		return server.execBGReWriteAOF(client, args) // 异步aof重写
	default:
		return server.execCommand(client, cmdLine) // db命令
	}
}

func (server *Server) CloseClient(client _interface.Client) {
	err := client.Close()
	if err != nil {
		logger.Warn("client close err: " + err.Error())
	}
	// 取消订阅
	channels := client.GetChannels()
	server.pubsub.UnSubscribe(client, channels)
	logger.Info(fmt.Sprintf("client [%s] closed successfully.", client.RemoteAddr()))
}

func (server *Server) Close() {
	if server.persister != nil {
		server.persister.Close()
	}
	logger.Info("redis server closed successfully.")
}

func (server *Server) isAuth(client _interface.Client) bool {
	if Config.RequirePass == "" {
		return true // 未设置密码
	}
	return client.GetPassword() == Config.RequirePass // 密码是否一致
}
