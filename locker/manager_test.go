package locker_test

import (
	"testing"
	"time"

	redis "github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func TestNotifyRunningShouldReturnErrorIfTaskAlreadyRunning(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "redis:6379", Password: "", DB: 0})
	lock := locker.New(client, time.Minute)

	taskName := "task-test"
	err := lock.NotifyRunning(taskName)
	assert.Nil(t, err)

	err = lock.NotifyRunning(taskName)
	assert.Equal(t, locker.ErrAlreadyRunning, err)
}
