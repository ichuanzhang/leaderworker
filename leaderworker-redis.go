package leaderworker

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

const (
	BaseKey      = "leader_worker"
	KeepAliveKey = "keep_alive"
	KeepRaceKey  = "keep_race"
)

var getExpireScript = redis.NewScript(`
local key = KEYS[1]
local value = ARGV[1]
local expire = tonumber(ARGV[2])
local val = redis.call("GET", key)

local ok = false
if value == val then
	ok = true
	redis.call("EXPIRE", key, expire)
end

return ok
`)

type Options struct {
	Host              string
	Port              int
	Password          string
	Db                int
	Id                string
	Namespace         string
	KeepAliveInterval time.Duration
	KeepRaceInterval  time.Duration
	KeepRaceExpire    time.Duration
}

type RedisLeaderWorker struct {
	id                string
	namespace         string
	keepAliveInterval time.Duration
	keepRaceInterval  time.Duration
	keepRaceExpire    time.Duration
	client            *redis.Client
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
}

func NewRedisLeaderWorker(opt *Options) (*RedisLeaderWorker, error) {
	client, err := newRedisClient(opt)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &RedisLeaderWorker{
		id:                opt.Id,
		namespace:         opt.Namespace,
		keepAliveInterval: opt.KeepAliveInterval,
		keepRaceInterval:  opt.KeepRaceInterval,
		keepRaceExpire:    opt.KeepRaceExpire,
		client:            client,
		ctx:               ctx,
		cancel:            cancel,
		wg:                sync.WaitGroup{},
	}

	return r, nil
}

func (r *RedisLeaderWorker) Keep() <-chan error {
	var errCh = make(chan error)

	r.wg.Add(1)
	ticker := time.NewTicker(r.keepAliveInterval)
	key := r.getKeepAliveKey()

	go func() {
		defer func() {
			r.wg.Done()
			ticker.Stop()
		}()

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				z := redis.Z{
					Score:  float64(time.Now().Unix()),
					Member: r.id,
				}
				_, err := r.client.ZAdd(r.ctx, key, &z).Result()
				if err != nil {
					errCh <- err
					continue
				}
			}
		}

	}()

	return errCh
}

func (r *RedisLeaderWorker) Race() (<-chan bool, <-chan error) {
	var (
		is    bool
		ch    = make(chan bool)
		errCh = make(chan error)
	)

	r.wg.Add(1)
	ticker := time.NewTicker(r.keepRaceInterval)
	key := r.getKeepRaceKey()

	go func() {
		defer func() {
			r.wg.Done()
			ticker.Stop()
		}()

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				if is {
					ok, err := getExpireScript.Run(r.ctx, r.client, []string{key}, r.id, r.keepRaceExpire/time.Second).Bool()
					if err != nil && !errors.Is(err, redis.Nil) {
						errCh <- err
					}
					if ok {
						ch <- is
						break
					}
				}
				if ok := r.client.SetNX(r.ctx, key, r.id, r.keepRaceExpire).Val(); ok {
					is = true
					ch <- is
					break
				}
				is = false
				ch <- is
			}
		}
	}()

	return ch, errCh
}

func (r *RedisLeaderWorker) List() ([]Node, error) {
	key := r.getKeepAliveKey()

	result, err := r.client.ZRangeWithScores(r.ctx, key, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var data = make([]Node, 0, len(result))
	for _, z := range result {
		id, ok := z.Member.(string)
		if !ok {
			continue
		}

		alive := Node{
			Id:        id,
			LastAlive: int64(z.Score),
		}

		data = append(data, alive)
	}

	return data, nil
}

func (r *RedisLeaderWorker) Remove(id ...string) (int64, error) {
	key := r.getKeepAliveKey()

	return r.client.ZRem(r.ctx, key, id).Result()
}

func (r *RedisLeaderWorker) Stop() {
	r.cancel()
	r.wg.Wait()
}

func newRedisClient(opt *Options) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", opt.Host, opt.Port),
		Password: opt.Password,
		DB:       opt.Db,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return rdb, err
	}

	return rdb, nil
}

func (r *RedisLeaderWorker) getKeepAliveKey() string {
	return fmt.Sprintf("%s:%s:%s", r.namespace, BaseKey, KeepAliveKey)
}

func (r *RedisLeaderWorker) getKeepRaceKey() string {
	return fmt.Sprintf("%s:%s:%s", r.namespace, BaseKey, KeepRaceKey)
}
