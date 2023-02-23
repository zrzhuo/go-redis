package main

import (
	"fmt"
	"go-redis/redis"
	"go-redis/tcp"
	"go-redis/utils/logger"
)

func main() {
	print("go-redis running...\n")
	redis.ParseConfig("redis.conf") // 从redis.conf中读取配置
	address := fmt.Sprintf("%s:%d", redis.Config.Bind, redis.Config.Port)
	err := tcp.MakeTcpServer(address, tcp.MakeRedisHandler()).Start()
	if err != nil {
		logger.Error(err)
		return
	}
}
