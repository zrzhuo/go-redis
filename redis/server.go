package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/resp/reply"
	"go-redis/utils/logger"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
)

type Server struct {
	databases []*atomic.Value // 若干个redis数据库
	persister *Persister      // AOF持久化
	pubsub    *Pubsub         // pub/sub
	lock      sync.Mutex      // 锁
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
	if Config.Appendonly {
		filename, fsync := Config.Appendfilename, Config.Appendfsync
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

func (server *Server) ExecWithLock(client _interface.Client, cmdLine _type.CmdLine) (reply _interface.Reply) {
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			reply = &Reply.UnknownErrReply{}
		}
	}()
	//上锁
	server.Lock()
	defer server.Unlock()
	cmd := strings.ToLower(string(cmdLine[0]))
	args := _type.Args(cmdLine[1:])
	// auth
	if cmd == "auth" {
		return execAuth(server, client, args)
	}
	if !server.isAuth(client) {
		return Reply.MakeErrReply("NOAUTH Authentication required.")
	}
	// 事务处理
	if client.IsTxState() && cmd != "exec" && cmd != "discard" {
		return server.handleTX(client, cmdLine)
	}
	// 分发命令
	sysCmd, ok := SysCmdRouter[cmd]
	if ok {
		return sysCmd.SysExec(server, client, args) // 执行系统命令
	}
	return server.execCommand(client, cmdLine) // 执行数据库命令
}

func (server *Server) ExecWithoutLock(client _interface.Client, cmdLine _type.CmdLine) (reply _interface.Reply) {
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			reply = &Reply.UnknownErrReply{}
		}
	}()
	cmd := strings.ToLower(string(cmdLine[0]))
	args := _type.Args(cmdLine[1:])
	// 分发命令
	sysCmd, ok := SysCmdRouter[cmd]
	if ok {
		return sysCmd.SysExec(server, client, args) // 执行系统命令
	}
	return server.execCommand(client, cmdLine) // 执行数据库命令
}

func (server *Server) execCommand(client _interface.Client, cmdLine _type.CmdLine) _interface.Reply {
	dbIdx := client.GetSelectDB()
	if dbIdx < 0 || dbIdx >= len(server.databases) {
		err := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.MakeErrReply(err)
	}
	db := server.databases[dbIdx].Load().(*Database)
	return db.Execute(client, cmdLine)
}

func (server *Server) handleTX(client _interface.Client, cmdLine _type.CmdLine) _interface.Reply {
	name := strings.ToLower(string(cmdLine[0]))
	sysCmd, ok := SysCmdRouter[name]
	if ok {
		// 参数个数是否满足要求
		if !checkArgNum(sysCmd.Arity, cmdLine) {
			errReply := Reply.MakeArgNumErrReply(name)
			client.AddTxError(errReply)
			return errReply
		}
		client.EnTxQueue(cmdLine)
		return Reply.MakeQueuedReply()
	}
	cmd, ok := CmdRouter[name]
	if ok {
		if !checkArgNum(cmd.Arity, cmdLine) {
			errReply := Reply.MakeArgNumErrReply(name)
			client.AddTxError(errReply)
			return errReply
		}
		client.EnTxQueue(cmdLine)
		return Reply.MakeQueuedReply()
	}
	errReply := Reply.MakeErrReply("unknown command '" + name + "'")
	client.AddTxError(errReply)
	return errReply
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
	if Config.Requirepass == "" {
		return true // 未设置密码
	}
	return client.GetPassword() == Config.Requirepass // 密码是否一致
}

func (server *Server) Lock() {
	server.lock.Lock()
}

func (server *Server) Unlock() {
	server.lock.Unlock()
}
