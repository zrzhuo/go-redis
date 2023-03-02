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

type Handler struct {
	server  _interface.Server
	clients sync.Map
	closing _sync.Boolean // 标志当前handler是否处于"closing"的状态
}

func MakeHandler() *Handler {
	server := redis.MakeServer()
	commands.RegisterAllCommand() // 注册所有命令
	return &Handler{
		server: server,
	}
}

func (handler *Handler) Handle(conn net.Conn) {
	if handler.closing.Get() {
		_ = conn.Close() // handler正处于closing状态，拒绝该连接
		return
	}

	// 包装为client，并记录到clients
	client := redis.NewClient(conn)
	handler.clients.Store(client, struct{}{})

	// handle
	parser := resp.MakeParser(conn)
	ch := parser.ParseCLI()
	for payload := range ch {
		if payload.Err != nil {
			// EOF错误，连接已断开
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF || strings.Contains(payload.Err.Error(), "use of closed network connection") {
				handler.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// 其他错误
			errReply := Reply.StandardError(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				handler.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
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
		result := handler.server.ExecWithLock(client, cmdLine)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(Reply.UnknownError().ToBytes())
		}
	}
}

func (handler *Handler) Close() error {
	logger.Info("handler shutting down...")
	handler.closing.Set(true) // 设置为closing状态
	handler.clients.Range(func(key any, val any) bool {
		client := key.(*redis.Client)
		err := client.Close() // 逐个关闭连接
		if err != nil {
			logger.Warn("client close err: " + err.Error())
		}
		return true
	})
	handler.server.Close() // 关闭数据库
	return nil
}

// 关闭指定连接
func (handler *Handler) closeClient(client *redis.Client) {
	handler.server.CloseClient(client)
	handler.clients.Delete(client)
}
