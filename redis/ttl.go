package redis

import (
	"go-redis/utils/timewheel"
	"time"
)

var TimeWheel = timewheel.MakeTimeWheel(time.Second, 60)

func init() {
	TimeWheel.Run()
}
