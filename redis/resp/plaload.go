package resp

import (
	"go-redis/interface"
)

type Payload struct {
	Data _interface.Reply
	Err  error
}
