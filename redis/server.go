package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
	"go-redis/utils/logger"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

type Server struct {
	databases []*atomic.Value // 若干个redis数据库
	persister *Persister      // AOF持久化
	pubsub    *Pubsub         // pub/sub
	txing     bool            // 正在执行事务
}

// NewServer 读取配置，创建server
func NewServer() *Server {
	server := &Server{}
	// 创建指定个数的db，默认为16
	dbNum := 16
	if Config.Databases > 0 {
		dbNum = Config.Databases
	}
	server.databases = make([]*atomic.Value, dbNum)
	for i := range server.databases {
		db := NewDatabase(i)
		holder := &atomic.Value{}
		holder.Store(db)
		server.databases[i] = holder
	}
	// pub/sub
	server.pubsub = NewPubsub()
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
		logger.Info("DB load from append only file...")
		persister.Listening() // 开启AOF监听
		server.persister = persister
	}
	return server
}

// MakeFakeServer 创建一个临时的server，用于AOF重写
func MakeFakeServer() *Server {
	server := &Server{}
	// 创建指定个数的db，默认为16
	dbNum := 16
	if Config.Databases > 0 {
		dbNum = Config.Databases
	}
	server.databases = make([]*atomic.Value, dbNum)
	for i := range server.databases {
		db := NewSimpleDatabase(i)
		holder := &atomic.Value{}
		holder.Store(db)
		server.databases[i] = holder
	}
	return server
}

func (server *Server) ExecCommand(client _interface.Client, cmdLine _type.CmdLine) (reply _interface.Reply) {
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			reply = &Reply.UnknownErrReply{}
		}
	}()
	// server正在执行事务，等待
	for server.IsTxing() {
		time.Sleep(1 * time.Millisecond)
	}

	cmd := strings.ToLower(string(cmdLine[0]))
	// 鉴权未通过，且当前命令不是auth命令
	if !server.isAuth(client) && cmd != "auth" {
		return Reply.StandardError("NOAUTH Authentication required.")
	}
	// 事务处理(client处于事务状态，且cmd不是事务相关命令)
	if client.IsTxState() && !IsTxCmd(cmd) {
		return server.handleTX(client, cmdLine)
	}
	// 分发命令
	_, ok := SysCmdRouter[cmd]
	if ok {
		return server.execSysCommand(client, cmdLine) // 执行系统命令
	} else {
		return server.execCommand(client, cmdLine) // 执行数据库命令
	}
}

func (server *Server) ExecForTX(client _interface.Client, cmdLine _type.CmdLine) (reply _interface.Reply) {
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			reply = &Reply.UnknownErrReply{}
		}
	}()
	cmd := strings.ToLower(string(cmdLine[0]))
	// 分发命令
	_, ok := SysCmdRouter[cmd]
	if ok {
		return server.execSysCommand(client, cmdLine) // 执行系统命令
	} else {
		return server.execCommand(client, cmdLine) // 执行数据库命令
	}
}

func (server *Server) ExecForAOF(client _interface.Client, cmdLine _type.CmdLine) (reply _interface.Reply) {
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			reply = &Reply.UnknownErrReply{}
		}
	}()
	cmd := strings.ToLower(string(cmdLine[0]))
	// 分发命令
	_, ok := SysCmdRouter[cmd]
	if ok {
		return server.execSysCommand(client, cmdLine) // 执行系统命令
	} else {
		dbIdx := client.GetSelectDB()
		db := server.databases[dbIdx].Load().(*Database)
		return db.QuickExecute(client, cmdLine) // 快速执行
	}
}

func (server *Server) execSysCommand(client _interface.Client, cmdLine _type.CmdLine) _interface.Reply {
	cmd := strings.ToLower(string(cmdLine[0]))
	args := _type.Args(cmdLine[1:])
	sysCmd, ok := SysCmdRouter[cmd]
	if !ok {
		return Reply.StandardError("unknown command '" + cmd + "'") // 不存在该命令
	}
	if !utils.CheckArgNum(sysCmd.Arity, cmdLine) {
		return Reply.ArgNumError(cmd) // 参数个数不满足要求
	}
	return sysCmd.Executor(server, client, args)
}

func (server *Server) execCommand(client _interface.Client, cmdLine _type.CmdLine) _interface.Reply {
	dbIdx := client.GetSelectDB()
	if dbIdx < 0 || dbIdx >= len(server.databases) {
		err := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.StandardError(err)
	}
	db := server.databases[dbIdx].Load().(*Database)
	return db.Execute(client, cmdLine)
}

func (server *Server) handleTX(client _interface.Client, cmdLine _type.CmdLine) _interface.Reply {
	name := strings.ToLower(string(cmdLine[0]))
	// system command
	sysCmd, ok := SysCmdRouter[name]
	if ok {
		// 检查参数个数
		if !utils.CheckArgNum(sysCmd.Arity, cmdLine) {
			errReply := Reply.ArgNumError(name)
			client.AddTxError(errReply)
			return errReply
		}
		client.EnTxQueue(cmdLine)
		return Reply.MakeQueuedReply()
	}
	// database command
	cmd, ok := CmdRouter[name]
	if ok {
		// 检查参数个数
		if !utils.CheckArgNum(cmd.Arity, cmdLine) {
			errReply := Reply.ArgNumError(name)
			client.AddTxError(errReply)
			return errReply
		}
		client.EnTxQueue(cmdLine)
		return Reply.MakeQueuedReply()
	}
	// unknown command
	errReply := Reply.StandardError("unknown command '" + name + "'")
	client.AddTxError(errReply)
	return errReply
}

func (server *Server) SetTxing(flag bool) {
	server.txing = flag
}

func (server *Server) IsTxing() bool {
	return server.txing
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

func (server *Server) getDatabase(dbIdx int) *Database {
	return server.databases[dbIdx].Load().(*Database)
}

func (server *Server) dataBaseCount() int {
	return len(server.databases)
}
