package main

import (
	"time"

	redis "gopkg.in/redis.v5"

	"github.com/perenecabuto/redis-dist-lock/locker"
)

func main() {
	client := redis.NewClient(&redis.Options{Addr: "redis:6379", Password: "", DB: 0})
	lock := locker.New(client, time.Minute)
	println(lock)
}
