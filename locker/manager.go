package locker

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	redis "github.com/go-redis/redis"
)

var (
	notificationTimeout = time.Minute
	ErrAlreadyRunning   = errors.New("Task already running")
)

type Locker interface {
	IsRunning(taskName string) (bool, error)
	GetHostRunning(taskName string) (string, error)
	NotifyRunning(taskName string) error
	NotifyStoppedRunning(taskName string) error
	RunWhenReady(ctx context.Context, taskName string, task func(context.Context)) error
}

type RedisLocker struct {
	client        *redis.Client
	retryInterval time.Duration
	hostname      string
}

func New(client *redis.Client, retryInterval time.Duration) *RedisLocker {
	hostname, _ := os.Hostname()
	return &RedisLocker{client, retryInterval, hostname}
}

func (m *RedisLocker) RunWhenReady(ctx context.Context, taskName string, task func(context.Context)) error {
	host, err := m.GetHostRunning(taskName)
	lockIsFree := err == redis.Nil
	if !lockIsFree && err != nil {
		return err
	}
	if !lockIsFree {
		if host == m.hostname {
			m.NotifyStoppedRunning(taskName)
		}
		retry := time.NewTimer(m.retryInterval)
		defer retry.Stop()
		select {
		case <-retry.C:
		case <-ctx.Done():
			return nil
		}
		return m.RunWhenReady(ctx, taskName, task)
	}

	err = m.NotifyRunning(taskName)
	if err == ErrAlreadyRunning {
		return m.RunWhenReady(ctx, taskName, task)
	}
	if err != nil {
		return err
	}
	log.Println("[RedisLocker]", "task <", taskName, "> running on", m.hostname)
	ticker := m.heartbeat(ctx, taskName)
	defer ticker.Stop()
	task(ctx)
	return nil
}

func (m *RedisLocker) IsRunning(taskName string) (bool, error) {
	res, err := m.client.Exists(taskName).Result()
	return res != 0, err
}

func (m *RedisLocker) GetHostRunning(taskName string) (string, error) {
	return m.client.Get(taskName).Result()
}

func (m *RedisLocker) NotifyRunning(taskName string) error {
	val, err := m.client.SetNX(taskName, m.hostname, notificationTimeout+time.Second).Result()
	if err != nil {
		return err
	}
	if !val {
		return ErrAlreadyRunning
	}
	return nil
}

func (m *RedisLocker) NotifyStoppedRunning(taskName string) error {
	return m.client.Del(taskName).Err()
}

func (m *RedisLocker) heartbeat(ctx context.Context, taskName string) *time.Ticker {
	ticker := time.NewTicker(notificationTimeout)
	go func() {
		defer func() {
			log.Println("[RedisLocker]", "stopping <", taskName, "> heartbeat on host", m.hostname)
			m.NotifyStoppedRunning(taskName)
		}()
		for {
			select {
			case _, ok := <-ticker.C:
				if !ok {
					return
				}
				m.client.Set(taskName, m.hostname, notificationTimeout+time.Second)
			case <-ctx.Done():
				return
			}
		}
	}()

	return ticker
}
