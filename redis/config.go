package redis

import (
	"bufio"
	"go-redis/utils/logger"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Bind           string `cfg:"bind"`           // 绑定ip
	Port           int    `cfg:"port"`           // 端口
	MaxClients     int    `cfg:"maxclients"`     // 同一时刻的最大客户端数
	Databases      int    `cfg:"databases"`      //数据库的数量
	AppendOnly     bool   `cfg:"appendonly"`     // 是否开启aof
	AppendFilename string `cfg:"appendfilename"` // aof文件名
	AppendFsync    string `cfg:"appendfsync"`    // aof文件写磁盘策略
	RequirePass    string `cfg:"requirepass"`    // 密码

	//RDBFilename       string   `cfg:"dbfilename"`
	//MasterAuth        string   `cfg:"masterauth"`
	//SlaveAnnouncePort int      `cfg:"slave-announce-port"`
	//SlaveAnnounceIP   string   `cfg:"slave-announce-ip"`
	//ReplTimeout       int      `cfg:"repl-timeout"`
	//Peers             []string `cfg:"peers"`
	//Self              string   `cfg:"self"`
}

var Config *ServerConfig

func init() {
	Config = &ServerConfig{
		Bind:       "127.0.0.1",
		Port:       6666,
		MaxClients: 128,
	}
}

// ParseConfig 从配置文件中读取配置
func ParseConfig(path string) {
	file, err := os.Open(path) // 打开文件
	if err != nil {
		panic(err)
	}
	defer file.Close() // 关闭文件
	Config = parse(file)
}

func parse(file io.Reader) *ServerConfig {
	config := &ServerConfig{}
	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && strings.TrimLeft(line, " ")[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}
	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}
