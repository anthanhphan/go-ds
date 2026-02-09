package idgen

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Clock abstracts the time source for the ID generator.
type Clock interface {
	// Now returns the current timestamp in milliseconds.
	Now() int64
}

// SystemClock uses the local system time.
type SystemClock struct{}

func (s *SystemClock) Now() int64 {
	return time.Now().UnixMilli()
}

// RedisClock uses Redis TIME command via Lua to get atomic time.
type RedisClock struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisClock(client *redis.Client) *RedisClock {
	return &RedisClock{
		client: client,
		ctx:    context.Background(),
	}
}

func (r *RedisClock) Now() int64 {
	// Use Redis TIME command which returns [seconds, microseconds]
	res, err := r.client.Time(r.ctx).Result()
	if err != nil {
		// Fallback to system clock if Redis is down (or log it)
		// For Snowflake, it's safer to panic or return error if we want strictness,
		// but here we just fallback for resilience in this demo.
		return time.Now().UnixMilli()
	}

	// seconds * 1000 + microseconds / 1000
	return res.Unix()*1000 + int64(res.Nanosecond())/1000000
}
