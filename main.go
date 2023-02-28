package main

import (
	"fmt"
	"go-redis/redis"
	"go-redis/tcp"
	"go-redis/utils/logger"
)

func main() {
	logger.Info("go-redis is running...")
	redis.InitConfig("redis.conf") // 从redis.conf中读取配置
	address := fmt.Sprintf("%s:%d", redis.Config.Bind, redis.Config.Port)
	handler := tcp.MakeHandler()
	server := tcp.MakeTcpServer(address, handler)
	err := server.Start()
	if err != nil {
		logger.Error(err)
		return
	}
}
