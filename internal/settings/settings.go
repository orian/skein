package settings

import "time"

var (
	HeartbeatInterval = 10 * time.Second
	LongPollTimeout   = 30 * time.Second
)
