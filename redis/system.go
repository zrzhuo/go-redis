package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
	"strconv"
	"strings"
	"time"
)

func init() {
	RegisterSysCommand("sleep", execSleep, 2) // sleep，用于测试
	RegisterSysCommand("ping", execPing, -1)
	RegisterSysCommand("config", execConfig, -2)
	RegisterSysCommand("select", execSelect, 2)

	RegisterSysCommand("flushdb", execFlushDB, 1)
	RegisterSysCommand("flushall", execFlushAll, 1)

	RegisterSysCommand("subscribe", execSubscribe, -2)
	RegisterSysCommand("unsubscribe", execUnSubscribe, 1)
	RegisterSysCommand("publish", execPublish, 3)

	RegisterSysCommand("rewriteaof", execReWriteAOF, 1)     // aof重写
	RegisterSysCommand("bgrewriteaof", execBGReWriteAOF, 1) // 异步aof重写

	RegisterSysCommand("multi", execMulti, 1)     // 开启事务
	RegisterSysCommand("exec", execExec, 1)       // 执行事务
	RegisterSysCommand("discard", execDiscard, 1) // 退出事务
	RegisterSysCommand("watch", execWatch, -2)
	RegisterSysCommand("unwatch", execUnWatch, 1)
}

func execSleep(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	st, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return Reply.StandardError("illegal integer.")
	}
	time.Sleep(time.Duration(st) * time.Second)
	return Reply.MakeStatusReply("sleep over")
}

/* ---- base ---- */

func execPing(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	size := len(args)
	if size == 0 {
		return Reply.MakePongReply()
	}
	if size == 1 {
		return Reply.MakeStatusReply(string(args[0]))
	}
	return Reply.ArgNumError("Ping")
}

func execAuth(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if Config.Requirepass == "" {
		return Reply.StandardError("no password is set.")
	}
	password := string(args[0])
	client.SetPassword(password)
	if password != Config.Requirepass {
		return Reply.StandardError("invalid password.")
	}
	return Reply.MakeOkReply()
}

func execSelect(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	dbIdx, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return Reply.StandardError("selected index is invalid")
	}
	if dbIdx >= len(server.databases) || dbIdx < 0 {
		msg := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.StandardError(msg)
	}
	client.SetSelectDB(dbIdx) // 修改client的dbIdx
	return Reply.MakeOkReply()
}

/* ---- Config ---- */

func execConfig(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	cmd := strings.ToLower(string(args[0]))
	if cmd == "get" {
		return execConfigGet(server, client, args)
	}
	if cmd == "set" {
		return execConfigSet(server, client, args)
	}
	return Reply.StandardError(fmt.Sprintf("unknown subcommand '%s'", cmd))
}

func execConfigGet(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	num := len(args)
	if num < 2 {
		return Reply.ArgNumError("config|get")
	}
	var result []string
	for i := 1; i < num; i++ {
		key := strings.ToLower(string(args[i]))
		val, ok := GetConfig(key)
		if !ok {
			continue
		}
		result = append(result, key)
		result = append(result, val)
	}
	return Reply.StringToArrayReply(result...)
}

func execConfigSet(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	num := len(args)
	if num < 2 || num%2 == 0 {
		return Reply.ArgNumError("config|set")
	}
	for i := 0; i < num/2; i++ {
		key := strings.ToLower(string(args[2*i+1]))
		val := strings.ToLower(string(args[2*i+2]))
		err := SetConfig(key, val)
		if err != nil {
			return Reply.StandardError(err.Error())
		}
	}
	return Reply.MakeOkReply()
}

/* ---- flush ---- */

func execFlushDB(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	dbIdx := client.GetSelectDB()
	db := server.GetDatabase(dbIdx)
	db.Flush()
	db.ToAOF(utils.ToCmd("flushdb", []byte(strconv.Itoa(dbIdx))))
	return Reply.MakeOkReply()
}

func execFlushAll(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	for i := 0; i < len(server.databases); i++ {
		db := server.databases[i].Load().(*Database)
		db.Flush()
		if i == 0 {
			db.ToAOF(utils.ToCmd("flushall"))
		}
	}
	return Reply.MakeOkReply()
}

/* ---- pub/sub ---- */

func execSubscribe(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	return server.pubsub.Subscribe(client, channels)
}

func execUnSubscribe(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	var channels []string
	if len(args) == 0 {
		channels = client.GetChannels() // 所有的channel
	} else {
		channels = make([]string, len(args)) // 指定的channel
		for i, arg := range args {
			channels[i] = string(arg)
		}
	}
	return server.pubsub.UnSubscribe(client, channels)
}

func execPublish(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	channel, message := string(args[0]), args[1]
	return server.pubsub.Publish(client, channel, message)
}

/* ---- AOF ---- */

func execReWriteAOF(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	err := server.persister.ReWrite()
	if err != nil {
		return Reply.StandardError(err.Error())
	}
	return Reply.MakeOkReply()
}

func execBGReWriteAOF(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	go func() {
		_ = server.persister.ReWrite()
	}()
	return Reply.MakeStatusReply("background aof rewriting started")
}

/* ---- transaction ---- */

func execWatch(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if client.IsTxState() {
		return Reply.StandardError("WATCH inside MULTI is not allowed")
	}
	client.InitWatch(server.DataBaseCount())
	dbIdx := client.GetSelectDB()
	db := server.GetDatabase(dbIdx)
	for i := 0; i < len(args); i++ {
		key := string(args[i])
		version := db.GetVersion(key)
		client.SetWatchKey(dbIdx, key, version)
	}
	return Reply.MakeOkReply()
}

func execUnWatch(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	client.DestroyWatch()
	return Reply.MakeOkReply()
}

func execMulti(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if client.IsTxState() {
		return Reply.StandardError("MULTI calls can not be nested")
	}
	client.SetTxState(true)
	return Reply.MakeOkReply()
}

func execExec(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if !client.IsTxState() {
		return Reply.StandardError("EXEC without MULTI")
	}
	// 执行事务期间禁止server执行其他命令
	server.SetTxing(true)
	defer server.SetTxing(false)
	// 解除client事务状态，并清空txQueue和txWatch
	defer client.SetTxState(false)
	defer client.ClearTxQueue()
	defer client.DestroyWatch()
	// 检查被watch的keys是否被更改
	for i, keys := range client.GetWatchKeys() {
		db := server.GetDatabase(i)
		for key, version := range keys {
			currVersion := db.GetVersion(key)
			if version != currVersion {
				return Reply.MakeNilBulkReply() // 已被修改，放弃事务执行
			}
		}
	}
	// 检查是否出现错误
	if len(client.GetTxError()) > 0 {
		return Reply.StandardError("EXECABORT Transaction discarded because of previous errors.")
	}
	// 执行
	cmdLines := client.GetTxQueue()
	replies := make([]string, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		reply := server.ExecWithoutLock(client, cmdLine)
		if Reply.IsErrorReply(reply) {
			break
		}
		replies = append(replies, string(reply.ToBytes()))
	}
	return Reply.StringToArrayReply(replies...)
}
func execDiscard(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if !client.IsTxState() {
		return Reply.StandardError("DISCARD without MULTI")
	}
	client.SetTxState(false)
	client.ClearTxQueue()
	client.DestroyWatch()
	return Reply.MakeOkReply()
}
