package tcp

import (
	_interface "go-redis/interface"
	"go-redis/redis"
	"go-redis/redis/commands"
	"go-redis/resp"
	Reply "go-redis/resp/reply"
	"go-redis/utils/logger"
	_sync "go-redis/utils/sync"
	"io"
	"net"
	"strings"
	"sync"
)

type RedisHandler struct {
	redisEngine _interface.DB
	connections sync.Map      // 并发安全的map，其key用于存放"活动中的连接"
	closing     _sync.Boolean // 原子类型的bool，标志当前handler是否处于"closing"的状态
}

func MakeRedisHandler() *RedisHandler {
	db := redis.MakeServer()
	// 注册所有命令
	commands.RegisterAllCommand()
	return &RedisHandler{
		redisEngine: db,
	}
}

// Handle 接收并执行命令
func (handler *RedisHandler) Handle(conn net.Conn) {
	if handler.closing.Get() {
		_ = conn.Close() // handler正处于closing状态，拒绝该连接
		return
	}

	// 包装为RedisConn，并存入activeConn
	redisConn := redis.NewRedisConn(conn)
	handler.connections.Store(redisConn, struct{}{})

	// handle
	parser := resp.MakeParser(redisConn.Conn)
	ch := parser.ParseCLI()
	for payload := range ch {
		if payload.Err != nil {
			// EOF错误，连接已断开
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				handler.closeRedisConn(redisConn)
				logger.Info("connection closed: " + redisConn.RemoteAddr())
				return
			}
			// 其他错误
			errReply := Reply.MakeErrReply(payload.Err.Error())
			_, err := redisConn.Write(errReply.ToBytes())
			if err != nil {
				handler.closeRedisConn(redisConn)
				logger.Info("connection closed: " + redisConn.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		// 构建commands
		reply, ok := payload.Data.(*Reply.ArrayReply)
		if !ok {
			logger.Error("wrong commands line: require multi bulk strings")
			continue
		}
		cmdLine := reply.Args
		// 执行命令
		result := handler.redisEngine.Exec(redisConn, cmdLine)
		if result != nil {
			_, _ = redisConn.Write(result.ToBytes())
		} else {
			_, _ = redisConn.Write(Reply.MakeUnknownErrReply().ToBytes())
		}
	}
}

// Close stops handler
func (handler *RedisHandler) Close() error {
	logger.Info("handler shutting down...")
	handler.closing.Set(true) // 设置为closing状态
	handler.connections.Range(func(key any, val any) bool {
		client := key.(*redis.Connection)
		_ = client.Close() // 逐个关闭连接
		return true
	})
	handler.redisEngine.Close() // 关闭数据库
	return nil
}

// 关闭指定连接
func (handler *RedisHandler) closeRedisConn(redisConn *redis.Connection) {
	_ = redisConn.Close()
	handler.redisEngine.AfterConnClose(redisConn)
	handler.connections.Delete(redisConn)
}
