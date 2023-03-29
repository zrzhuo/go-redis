package redis

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"strings"
)

const (
	ReadWrite = 0
	ReadOnly  = 1
)

/* ---- database command ---- */

type Executor func(db *Database, args _type.Args) _interface.Reply

type keysFind func(args _type.Args) ([]string, []string)

type command struct {
	Executor Executor
	keysFind keysFind
	Arity    int // 大于等于零时表示参数个数，小于零时表示参数个数的最小值
	Status   int // 当前命令是读命令还是写命令
}

var CmdRouter = make(map[string]*command)

func RegisterCommand(name string, executor Executor, keysFind keysFind, arity int, status int) {
	name = strings.ToLower(name)
	CmdRouter[name] = &command{
		Executor: executor,
		keysFind: keysFind,
		Arity:    arity,
		Status:   status,
	}
}

/* ---- system command ---- */

type SysExecutor func(server *Server, client _interface.Client, args _type.Args) _interface.Reply

type sysCommand struct {
	Executor SysExecutor
	Arity    int // 大于等于零时表示参数个数，小于零时表示参数个数的最小值
}

var SysCmdRouter = make(map[string]*sysCommand)

func RegisterSysCommand(name string, sysExec SysExecutor, arity int) {
	name = strings.ToLower(name)
	SysCmdRouter[name] = &sysCommand{
		Executor: sysExec,
		Arity:    arity,
	}
}

/* ---- 事务相关的命令 ---- */

var TxCmd = map[string]bool{
	"multi":   true,
	"exec":    true,
	"discard": true,
	"watch":   true,
	"unwatch": true,
}

func IsTxCmd(cmd string) bool {
	_, ok := TxCmd[cmd]
	return ok
}
