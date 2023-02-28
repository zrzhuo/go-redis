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

type Execute func(db *Database, args _type.Args) _interface.Reply

type keysFind func(args _type.Args) ([]string, []string)

//type Undo func(db *Database, args _type.Args) []_type.CmdLine

type command struct {
	Execute  Execute
	keysFind keysFind
	//undo     Undo
	Arity  int // 大于等于零时表示参数个数，小于零时表示参数个数的最小值
	Status int
}

var CmdRouter = make(map[string]*command)

func RegisterCommand(name string, execute Execute, keysFind keysFind, arity int, status int) {
	name = strings.ToLower(name)
	CmdRouter[name] = &command{
		Execute:  execute,
		keysFind: keysFind,
		Arity:    arity,
		Status:   status,
	}
}

/* ---- system command ---- */

type SysExec func(server *Server, client _interface.Client, args _type.Args) _interface.Reply

type sysCommand struct {
	SysExec SysExec
	Arity   int // 大于等于零时表示参数个数，小于零时表示参数个数的最小值
}

var SysCmdRouter = make(map[string]*sysCommand)

func RegisterSysCommand(name string, sysExec SysExec, arity int) {
	name = strings.ToLower(name)
	SysCmdRouter[name] = &sysCommand{
		SysExec: sysExec,
		Arity:   arity,
	}
}
