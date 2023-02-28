package redis

import (
	"bufio"
	"errors"
	"fmt"
	"go-redis/utils/logger"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type ServerConfig struct {
	// 为进行反射，所有字段都仅有首字母大写
	Bind        string // 绑定ip
	Port        int    // 端口
	Maxclients  int    // 同一时刻的最大客户端数
	Databases   int    //数据库的数量
	Requirepass string // 密码

	Appendonly     bool   // 是否开启aof
	Appendfilename string // aof文件名
	Appendfsync    string // aof文件写磁盘策略

	//MasterAuth        string   `cfg:"masterauth"`
	//SlaveAnnouncePort int      `cfg:"slave-announce-port"`
	//SlaveAnnounceIP   string   `cfg:"slave-announce-ip"`
	//ReplTimeout       int      `cfg:"repl-timeout"`
	//Peers             []string `cfg:"peers"`
	//Self              string   `cfg:"self"`
}

// Config 全局配置变量
var Config = &ServerConfig{
	Bind:        "127.0.0.1",
	Port:        6666,
	Maxclients:  128,
	Requirepass: "",
	Appendonly:  false,
}

var ConfigType = reflect.TypeOf(Config).Elem()

var ConfigValue = reflect.ValueOf(Config).Elem()

func InitConfig(path string) {
	// 打开文件
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close() // 关闭文件
	// 读取文件
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// 忽略注释(#开头)
		if len(line) > 0 && strings.TrimLeft(line, " ")[0] == '#' {
			continue
		}
		// 解析配置行
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 {
			key := strings.ToLower(line[0:pivot])
			val := strings.Trim(line[pivot+1:], " ")
			// 注入config
			err = SetConfig(key, val)
			if err != nil {
				logger.Warn(err.Error())
			}
		}
	}
	if err = scanner.Err(); err != nil {
		logger.Fatal(err)
	}
}

func SetConfig(key string, val string) error {
	name := strings.ToUpper(key[:1]) + strings.ToLower(key[1:])
	fieldVal := ConfigValue.FieldByName(name)
	field, ok := ConfigType.FieldByName(name)
	if !ok {
		return errors.New(fmt.Sprintf("unknown config option '%s'", name))
	}
	switch field.Type.Kind() {
	case reflect.Bool:
		fieldVal.SetBool("yes" == val)
	case reflect.String:
		fieldVal.SetString(val)
	case reflect.Int:
		intValue, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid value for config option '%s'", name))
		}
		fieldVal.SetInt(intValue)
	case reflect.Slice:
		if field.Type.Elem().Kind() == reflect.String {
			slice := strings.Split(val, ",")
			fieldVal.Set(reflect.ValueOf(slice))
		}
	}
	return nil
}

func GetConfig(key string) (string, bool) {
	name := strings.ToUpper(key[:1]) + strings.ToLower(key[1:])
	fieldVal := ConfigValue.FieldByName(name)
	field, ok := ConfigType.FieldByName(name)
	if !ok {
		return "", false
	}
	var val string
	switch field.Type.Kind() {
	case reflect.Bool:
		val = strconv.FormatBool(fieldVal.Bool())
	case reflect.String:
		val = fieldVal.String()
	case reflect.Int:
		val = strconv.FormatInt(fieldVal.Int(), 10)
	}
	return val, true
}
