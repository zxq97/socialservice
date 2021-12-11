package server

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"socialservice/util/concurrent"
)

func follow(ctx context.Context, uid, toUID int64) error {
	err := dbFollow(ctx, uid, toUID)
	if err != nil {
		return err
	}
	return cacheFollow(ctx, uid, toUID)
}

func unfollow(ctx context.Context, uid, toUID int64) error {
	err := dbUnfollow(ctx, uid, toUID)
	if err != nil {
		return err
	}
	return cacheUnfollow(ctx, uid, toUID)
}

func getFollow(ctx context.Context, uid, lastID, offset int64) ([]int64, bool, error) {
	if offset == 0 {
		offset = DefaultOffset
	}
	key := fmt.Sprintf(RedisKeyZFollow, uid)
	uids, hasMore, err := cacheGetFollow(ctx, key, lastID, offset)
	if err != nil {
		var utMap map[int64]int64
		uids, utMap, err = dbGetFollow(ctx, uid)
		if err != nil {
			return nil, false, err
		}
		cacheSetFollow(ctx, key, uids, utMap)
		uids = uids[:offset]
		if len(uids) > int(offset) {
			hasMore = true
		}
	}
	return uids, hasMore, nil
}

func getFollower(ctx context.Context, uid, lastID, offset int64) ([]int64, bool, error) {
	if offset == 0 {
		offset = DefaultOffset
	}
	key := fmt.Sprintf(RedisKeyZFollower, uid)
	uids, hasMore, err := cacheGetFollow(ctx, key, lastID, offset)
	if err != nil {
		var utMap map[int64]int64
		uids, utMap, err = dbGetFollower(ctx, uid)
		if err != nil {
			return nil, false, err
		}
		cacheSetFollow(ctx, key, uids, utMap)
		uids = uids[:offset]
		if len(uids) > int(offset) {
			hasMore = true
		}
	}
	return uids, hasMore, nil

}

func getFollowCount(ctx context.Context, uid int64) (int64, int64, error) {
	followCnt, followerCnt, err := cacheGetFollowCount(ctx, uid)
	if err != nil {
		followCnt, followerCnt, err = dbGetFollowCount(ctx, uid)
		if err != nil {
			return 0, 0, err
		}
		concurrent.Go(func() {
			cacheSetFollowCount(ctx, uid, followCnt, followerCnt)
		})
	}
	return followCnt, followerCnt, nil
}

func followTopic(ctx context.Context, uid, topicID int64) error {
	err := dbFollowTopic(ctx, uid, topicID)
	if err != nil {
		return err
	}
	return cacheFollowTopic(ctx, uid, topicID)
}

func unfollowTopic(ctx context.Context, uid, topicID int64) error {
	err := dbUnfollow(ctx, uid, topicID)
	if err != nil {
		return err
	}
	return cacheUnfollowTopic(ctx, uid, topicID)
}

func getFollowTopic(ctx context.Context, uid, lastID, offset int64) ([]int64, bool, error) {
	if offset == 0 {
		offset = DefaultOffset
	}
	key := fmt.Sprintf(RedisKeyZFollowTopic, uid)
	uids, hasMore, err := cacheGetFollow(ctx, key, lastID, offset)
	if err != nil {
		var utMap map[int64]int64
		uids, utMap, err = dbGetFollowTopic(ctx, uid)
		if err != nil {
			return nil, false, err
		}
		cacheSetFollow(ctx, key, uids, utMap)
		uids = uids[:offset]
		if len(uids) > int(offset) {
			hasMore = true
		}
	}
	return uids, hasMore, nil
}

func getFollowTopicCount(ctx context.Context, uid int64) (int64, error) {
	followCnt, err := cacheGetFollowTopicCount(ctx, uid)
	if err != nil {
		followCnt, err = dbGetFollowTopicCount(ctx, uid)
		if err != nil {
			return 0, err
		}
		concurrent.Go(func() {
			cacheSetFollowTopicCount(ctx, uid, followCnt)
		})
	}
	return followCnt, nil
}

func getAllFollow(ctx context.Context, uid int64, cursor uint64) ([]int64, uint64, error) {
	key := fmt.Sprintf(RedisKeyZFollow, uid)
	uids, c, err := getAllStream(ctx, key, cursor)
	if err == redis.Nil {
		uids, utMap, err := dbGetFollow(ctx, uid)
		if err != nil {
			return nil, 0, err
		}
		concurrent.Go(func() {
			cacheSetFollow(ctx, key, uids, utMap)
		})
		return uids, 0, nil
	}
	return uids, c, err
}

func getAllFollower(ctx context.Context, uid int64, cursor uint64) ([]int64, uint64, error) {
	key := fmt.Sprintf(RedisKeyZFollow, uid)
	uids, c, err := getAllStream(ctx, key, cursor)
	if err == redis.Nil {
		uids, utMap, err := dbGetFollow(ctx, uid)
		if err != nil {
			return nil, 0, err
		}
		concurrent.Go(func() {
			cacheSetFollow(ctx, key, uids, utMap)
		})
		return uids, 0, nil
	}
	return uids, c, err
}
