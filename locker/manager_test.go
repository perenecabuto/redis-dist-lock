package locker_test

import (
	"context"
	"testing"
	"time"

	redis "github.com/go-redis/redis"
	"github.com/perenecabuto/redis-dist-lock/locker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifyRunningShouldReturnErrorIfTaskAlreadyRunning(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	client.FlushAll()

	lock := locker.New(client, time.Minute)

	taskName := "task-test"
	err := lock.NotifyRunning(taskName)
	require.NoError(t, err)

	err = lock.NotifyRunning(taskName)
	assert.Equal(t, locker.ErrAlreadyRunning, err)
}

func TestNotifyRunningShouldCleanWhenItWasRunningInTheSameHost(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	client.FlushAll()
	interval := time.Millisecond * 10
	lock := locker.New(client, interval)

	taskName := "task-test"
	err := lock.NotifyRunning(taskName)
	require.NoError(t, err)

	running, err := lock.IsRunning(taskName)
	require.NoError(t, err)
	assert.True(t, running)

	ctx := context.Background()
	start := time.Now()
	var delta time.Duration
	err = lock.RunWhenReady(ctx, taskName, func(context.Context) {
		delta = time.Now().Sub(start)
	})
	require.NoError(t, err)

	deltaHappensInExpectedInterval := delta >= interval && delta <= (interval*2)
	assert.True(t, deltaHappensInExpectedInterval)
}
