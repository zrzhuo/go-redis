package utils

import (
	"errors"
	_type "go-redis/interface/type"
	"strconv"
	"time"
)

func ToCmdLine(name string, args ...[]byte) _type.CmdLine {
	cmdLine := make([][]byte, len(args)+1)
	cmdLine[0] = []byte(name)
	for i, s := range args {
		cmdLine[i+1] = s
	}
	return cmdLine
}

func ToExpireCmd(key string, expireAt time.Time) _type.CmdLine {
	ttl := strconv.FormatInt(expireAt.UnixNano()/1e6, 10)
	return ToCmdLine("PExpireAT", []byte(key), []byte(ttl))
}

func ParseCmds(line []byte) ([][]byte, error) {
	cmdLine, arg := make([][]byte, 0), make([]byte, 0)
	num := 0
	for i, ch := range line {
		if ch == ' ' {
			if num%2 == 0 {
				cmdLine = append(cmdLine, arg)
				arg = make([]byte, 0)
			} else {
				arg = append(arg, ch)
			}
		} else if ch == '"' {
			if i > 0 && line[i-1] == '\\' {
				arg = append(arg, ch)
			} else {
				num++
				if num%2 == 1 && i > 0 && line[i-1] == '"' {
					return nil, errors.New("invalid argument(s)")
				}
			}
		} else {
			arg = append(arg, ch)
		}
	}
	cmdLine = append(cmdLine, arg)
	return cmdLine, nil
}
