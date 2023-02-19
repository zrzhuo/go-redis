package database

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"strings"
)

type Execute func(db *Database, args _type.Args) _interface.Reply

type Prepare func(args _type.Args) ([]string, []string)

//type Undo func(db *Database, args _type.Args) []_type.CmdLine

type command struct {
	Execute Execute
	Prepare Prepare
	//undo     Undo
	Arity  int // allow number of args, Arity < 0 means len(args) >= -arity
	Status int
}

const (
	ReadWrite = 0
	ReadOnly  = 1
)

// Commands 存放所有命令
var Commands = make(map[string]*command)

func RegisterCommand(name string, execute Execute, prepare Prepare, arity int, status int) {
	name = strings.ToLower(name)
	Commands[name] = &command{
		Execute: execute,
		Prepare: prepare,
		Arity:   arity,
		Status:  status,
	}
}

func IsExisted(name string) bool {
	_, existed := Commands[name]
	return existed
}

func IsReadOnly(name string) bool {
	name = strings.ToLower(name)
	cmd, existed := Commands[name]
	if !existed {
		return false
	}
	return cmd.Status == ReadOnly
}
