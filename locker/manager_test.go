package locker_test

import (
	"testing"
	"time"

	redis "github.com/go-redis/redis"
	"github.com/perenecabuto/redis-dist-lock/locker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifyRunningShouldReturnErrorIfTaskAlreadyRunning(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	lock := locker.New(client, time.Minute)

	taskName := "task-test"
	err := lock.NotifyRunning(taskName)
	require.NoError(t, err)

	err = lock.NotifyRunning(taskName)
	assert.Equal(t, locker.ErrAlreadyRunning, err)
}
