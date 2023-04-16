package _interface

import (
	"net"
)

type Handler interface {
	Handle(conn net.Conn)
	Close() error
}
