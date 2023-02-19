package _interface

import (
	"context"
	"net"
)

type HandleFunc func(ctx context.Context, conn net.Conn)

type Handler interface {
	Handle(conn net.Conn)
	Close() error
}
