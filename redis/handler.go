package redis

import (
	"go-redis/database"
	_interface "go-redis/interface"
	"go-redis/redis/resp"
	"go-redis/redis/resp/reply"
	"go-redis/utils/logger"
	_sync "go-redis/utils/sync"
	"io"
	"net"
	"strings"
	"sync"
)

type Handler struct {
	engine     _interface.DB
	activeConn sync.Map      // *client -> placeholder  // 并发安全的map，其key用于存放"活动中的连接"
	closing    _sync.Boolean // refusing new client and new request  // 原子类型的bool，标志当前handler是否处于"closing"的状态
}

func MakeHandler() *Handler {
	db := database.MakeServer()
	return &Handler{
		engine: db,
	}
}

// Handle 接收并执行命令
func (handler *Handler) Handle(conn net.Conn) {
	if handler.closing.Get() {
		_ = conn.Close() // handler正处于closing状态，拒绝该连接
		return
	}

	// 建立RedisConn，并存入activeConn
	redisConn := NewRedisConn(conn)
	handler.activeConn.Store(redisConn, struct{}{})

	// handle
	parser := resp.MakeParser(redisConn.conn)
	ch := parser.ParseSteam()
	for payload := range ch {
		if payload.Err != nil {
			// EOF错误，连接已断开
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				handler.closeRedisConn(redisConn)
				logger.Info("connection closed: " + redisConn.RemoteAddr())
				return
			}
			// 其他错误
			errReply := reply.MakeErrReply(payload.Err.Error())
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
		rep, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("wrong commands: require multi bulk strings")
			continue
		}
		commands := rep.Args
		// 执行命令
		result := handler.engine.Exec(redisConn, commands)
		if result != nil {
			_, _ = redisConn.Write(result.ToBytes())
		} else {
			_, _ = redisConn.Write([]byte("-ERR unknown\r\n"))
		}
	}
}

// Close stops handler
func (handler *Handler) Close() error {
	logger.Info("handler shutting down...")
	handler.closing.Set(true) // 设置为closing状态
	handler.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*Connection)
		_ = client.Close() // 逐个关闭连接
		return true
	})
	handler.engine.Close() // 关闭数据库
	return nil
}

// 关闭指定连接
func (handler *Handler) closeRedisConn(redisConn *Connection) {
	_ = redisConn.Close()
	handler.engine.AfterConnClose(redisConn)
	handler.activeConn.Delete(redisConn)

}
