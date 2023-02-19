package main

import (
	"go-redis/database/commands"
	"go-redis/redis"
	"go-redis/tcp"
	"go-redis/utils/logger"
	"time"
)

func init() {
	commands.RegisterAllCommand()
}

var tcpCfg = tcp.Config{
	Address:    "localhost:6666",
	MaxConnect: 10,
	Timeout:    10 * time.Second,
}

//var echoHandler = tcp.MakeEchoHandler()

var redisHandler = redis.MakeHandler()

func main() {
	print("go-redis running...")
	tcpServer := tcp.MakeTcpServer(tcpCfg, redisHandler)
	err := tcpServer.ListenAndServeWithSignal()
	if err != nil {
		logger.Error(err)
		return
	}
}
