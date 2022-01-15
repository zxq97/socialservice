package server

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"socialservice/util/cast"
	"socialservice/util/constant"
	"time"
)

const (
	SleepTime     = 500 * time.Millisecond
	DefaultOffset = 10

	RedisKeyFollowCountTTL = 5 * time.Second

	RedisKeyFollowCount      = "social_service_follow_count_%v"       // uid
	RedisKeyFollowerCount    = "social_service_follower_count_%v"     // uid
	RedisKeyFollowTopicCount = "social_service_follow_topic_count_%v" //uid
	RedisKeyZFollow          = "social_service_follow_%v"             // uid follow_uid ctime
	RedisKeyZFollower        = "social_service_follower_%v"           // uid follower_uid ctime
	RedisKeyZFollowTopic     = "social_service_follow_topic_%v"       // uid topic_id
)

func cacheFollow(ctx context.Context, uid, toUID int64) error {
	key := fmt.Sprintf(RedisKeyZFollow, uid)
	fKey := fmt.Sprintf(RedisKeyZFollower, toUID)
	cKey := fmt.Sprintf(RedisKeyFollowCount, uid)
	cfKey := fmt.Sprintf(RedisKeyFollowerCount, toUID)
	now := float64(time.Now().Unix())
	pipe := redisCli.Pipeline()
	pipe.ZAdd(key, redis.Z{Member: toUID, Score: now})
	pipe.ZAdd(fKey, redis.Z{Member: uid, Score: now})
	pipe.Incr(cKey)
	pipe.Incr(cfKey)
	_, err := pipe.Exec()
	if err != nil {
		excLog.Printf("ctx %v follow pipeline uid %v to_uid %v err %v", ctx, uid, toUID, err)
	}
	return err
}

func cacheUnfollow(ctx context.Context, uid, toUID int64) error {
	key := fmt.Sprintf(RedisKeyZFollow, uid)
	fKey := fmt.Sprintf(RedisKeyZFollower, toUID)
	cKey := fmt.Sprintf(RedisKeyFollowCount, uid)
	cfKey := fmt.Sprintf(RedisKeyFollowerCount, toUID)
	pipe := redisCli.Pipeline()
	pipe.ZRem(key, toUID)
	pipe.ZRem(fKey, uid)
	pipe.Decr(cKey)
	pipe.Decr(cfKey)
	_, err := pipe.Exec()
	if err != nil {
		excLog.Printf("ctx %v unfollow pipeline uid %v to_uid %v err %v", ctx, uid, toUID, err)
	}
	return err
}

func cacheFollowTopic(ctx context.Context, uid, topicID int64) error {
	key := fmt.Sprintf(RedisKeyZFollowTopic, uid)
	cKey := fmt.Sprintf(RedisKeyFollowTopicCount, uid)
	pipe := redisCli.Pipeline()
	pipe.ZAdd(key, redis.Z{Member: topicID, Score: float64(time.Now().Unix())})
	pipe.Incr(cKey)
	_, err := pipe.Exec()
	if err != nil {
		excLog.Printf("ctx %v cacheFollowTopic uid %v topic_id %v err %v", ctx, uid, topicID, err)
	}
	return err
}

func cacheUnfollowTopic(ctx context.Context, uid, topicID int64) error {
	key := fmt.Sprintf(RedisKeyZFollowTopic, uid)
	cKey := fmt.Sprintf(RedisKeyFollowTopicCount, uid)
	pipe := redisCli.Pipeline()
	pipe.ZRem(key, topicID)
	pipe.Decr(cKey)
	_, err := pipe.Exec()
	if err != nil {
		excLog.Printf("ctx %v cacheUnfollowTopic uid %v topic_id %v err %v", ctx, uid, topicID, err)
	}
	return err
}

func cacheGetFollowCount(ctx context.Context, uid int64) (int64, int64, error) {
	key := fmt.Sprintf(RedisKeyFollowCount, uid)
	fKey := fmt.Sprintf(RedisKeyFollowerCount, uid)
	val, err := redisCli.MGet(key, fKey).Result()
	if err != nil || len(val) != 2 {
		excLog.Printf("ctx %v get follow count uid %v err %v", ctx, uid, err)
		return 0, 0, err
	}

	followCount := cast.ParseInt(val[0].(string), 0)
	followerCount := cast.ParseInt(val[1].(string), 0)
	return followCount, followerCount, nil
}

func cacheSetFollowCount(ctx context.Context, uid, followCount, followerCount int64) {
	key := fmt.Sprintf(RedisKeyFollowCount, uid)
	fKey := fmt.Sprintf(RedisKeyFollowerCount, uid)
	err := redisCli.MSet(key, followCount, fKey, followerCount).Err()
	if err != nil {
		excLog.Printf("ctx %v set follow count uid %v err %v", ctx, uid, err)
	}
}

func cacheGetFollowTopicCount(ctx context.Context, uid int64) (int64, error) {
	key := fmt.Sprintf(RedisKeyFollowTopicCount, uid)
	val, err := redisCli.Get(key).Result()
	if err != nil && err != redis.Nil {
		excLog.Printf("ctx %v cacheGetFollowTopicCount uid %v err %v", ctx, uid, err)
		return 0, err
	}
	return cast.ParseInt(val, 0), nil
}

func cacheSetFollowTopicCount(ctx context.Context, uid, topicCnt int64) {
	key := fmt.Sprintf(RedisKeyFollowTopicCount, uid)
	err := redisCli.Set(key, topicCnt, RedisKeyFollowCountTTL).Err()
	if err != nil {
		excLog.Printf("ctx %v cacheSetFollowTopicCount uid %v topic_count %v err %v", ctx, uid, topicCnt)
	}
}

func cacheGetFollow(ctx context.Context, key string, cursor, offset int64) ([]int64, bool, error) {
	val, err := redisCli.ZRevRange(key, cursor, cursor+offset).Result()
	if err != nil {
		excLog.Printf("ctx %v cache get key %v cursor %v err %v", ctx, key, cursor, err)
		return nil, false, err
	}
	var hasMore bool
	if int64(len(val)) > offset {
		hasMore = true
	}
	uids := make([]int64, 0, offset)
	for _, v := range val[:len(val)-1] {
		uid := cast.ParseInt(v, 0)
		uids = append(uids, uid)
	}
	return uids, hasMore, nil
}

func cacheSetFollow(ctx context.Context, key string, uids []int64, utMap map[int64]int64) {
	for i := 0; i < len(uids); i += constant.BatchSize {
		z := make([]redis.Z, 0, constant.BatchSize)
		left := i
		right := i + constant.BatchSize
		if right > len(uids) {
			right = len(uids)
		}
		for j := left; j < right; j++ {
			z = append(z, redis.Z{Member: uids[i], Score: float64(utMap[uids[i]])})
		}
		err := redisCli.ZAdd(key, z...).Err()
		if err != nil {
			excLog.Printf("ctx %v set follow z %v err %v", ctx, z, err)
			continue
		}
		time.Sleep(SleepTime)
	}
}

func getAllStream(ctx context.Context, key string, cursor uint64) ([]int64, uint64, error) {
	var (
		vals []string
		err  error
	)
	vals, cursor, err = redisCli.ZScan(key, cursor, "", constant.BatchSize).Result()
	if err != nil {
		excLog.Printf("ctx %v getAllStream key %v cursor %v err %v", ctx, key, cursor, err)
		return nil, 0, err
	}
	uids := make([]int64, 0, len(vals))
	for _, v := range vals {
		uids = append(uids, cast.ParseInt(v, 0))
	}
	return uids, cursor, nil
}
