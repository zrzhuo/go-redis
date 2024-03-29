package utils

import (
	"errors"
	Dict "go-redis/datastruct/dict"
	List "go-redis/datastruct/list"
	Set "go-redis/datastruct/set"
	ZSet "go-redis/datastruct/zset"
	_type "go-redis/interface/type"
	Reply "go-redis/resp/reply"
	"strconv"
	"time"
)

// ParseCmds 将一行输入解析为cmdLine，""中的内容为一个参数
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

// ParseRange 解析边界，返回的边界一律符合左闭右开[a, b)
func ParseRange(start int, end int, size int) (int, int) {
	// 解析start
	if start < -size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return -1, -1 // 不允许start超出size-1
	}
	// 解析stop
	if end < -size {
		end = 0
	} else if end < 0 {
		end = size + end
	} else if end >= size {
		end = size // 允许end超出size-1
	}
	// stop小于start
	if end < start {
		end = start
	}
	return start, end
}

// CheckArgNum 检查参数个数是否满足要求
func CheckArgNum(arity int, cmdLine _type.CmdLine) bool {
	argNum := len(cmdLine)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func ToCmd(name string, args ...[]byte) _type.CmdLine {
	cmdLine := make([][]byte, len(args)+1)
	cmdLine[0] = []byte(name)
	for i, s := range args {
		cmdLine[i+1] = s
	}
	return cmdLine
}

func StringToCmd(name string, args ...string) _type.CmdLine {
	cmdLine := make([][]byte, len(args)+1)
	cmdLine[0] = []byte(name)
	for i, s := range args {
		cmdLine[i+1] = []byte(s)
	}
	return cmdLine
}

func ToExpireCmd(key string, expireAt time.Time) _type.CmdLine {
	ttl := strconv.FormatInt(expireAt.UnixNano()/1e6, 10)
	return ToCmd("PExpireAT", []byte(key), []byte(ttl))
}

func EntityToCmd(key string, entity *_type.Entity) *Reply.ArrayReply {
	switch data := entity.Data.(type) {
	case []byte:
		return stringToCmd(key, data)
	case List.List[[]byte]:
		return listToCmd(key, data)
	case Set.Set[string]:
		return setToCmd(key, data)
	case ZSet.ZSet[string]:
		return zSetToCmd(key, data)
	case Dict.Dict[string, []byte]:
		return hashToCmd(key, data)
	default:
		return nil
	}
}

func ExpireToCmd(key string, expireTime *time.Time) *Reply.ArrayReply {
	expire := strconv.FormatInt(expireTime.UnixNano()/1e6, 10)
	return Reply.StringToArrayReply("PExpireAT", key, expire)
}

func stringToCmd(key string, bytes []byte) *Reply.ArrayReply {
	return Reply.StringToArrayReply("Set", key, string(bytes))
}

func listToCmd(key string, list List.List[[]byte]) *Reply.ArrayReply {
	vals := list.Range(0, list.Len())
	cmdLine := make([][]byte, 2+list.Len())
	cmdLine[0] = []byte("LPush")
	cmdLine[1] = []byte(key)
	for i, val := range vals {
		cmdLine[i+2] = val
	}
	return Reply.NewArrayReply(cmdLine)
}

func setToCmd(key string, set Set.Set[string]) *Reply.ArrayReply {
	members := set.Members()
	cmdLine := make([][]byte, 2+set.Len())
	cmdLine[0] = []byte("SAdd")
	cmdLine[1] = []byte(key)
	for i, val := range members {
		cmdLine[i+2] = []byte(val)
	}

	return Reply.NewArrayReply(cmdLine)
}

func zSetToCmd(key string, zset ZSet.ZSet[string]) *Reply.ArrayReply {
	args := make([][]byte, 2+zset.Len()*2)
	args[0] = []byte("ZAdd")
	args[1] = []byte(key)
	i := 0
	consumer := func(member string, score float64) bool {
		args[2+i*2] = []byte(strconv.FormatFloat(score, 'f', -1, 64))
		args[3+i*2] = []byte(member)
		i++
		return true
	}
	zset.ForEach(0, zset.Len(), true, consumer)
	return Reply.NewArrayReply(args)
}

func hashToCmd(key string, hash Dict.Dict[string, []byte]) *Reply.ArrayReply {
	args := make([][]byte, 2+hash.Len()*2)
	args[0] = []byte("HMSet")
	args[1] = []byte(key)
	i := 0
	consumer := func(field string, val []byte) bool {
		args[2+i*2] = []byte(field)
		args[3+i*2] = val
		i++
		return true
	}
	hash.ForEach(consumer)
	return Reply.NewArrayReply(args)
}
