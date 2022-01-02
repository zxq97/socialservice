package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"log"
	"socialservice/conf"
	"socialservice/rpc/social/pb"
	"socialservice/util/constant"
)

type SocialService struct {
}

var (
	mcCli    *memcache.Client
	redisCli redis.Cmdable
	dbCli    *gorm.DB
	slaveCli *gorm.DB
)

func InitService(config *conf.Conf) error {
	var err error
	log.SetFlags(log.Ldate | log.Lshortfile | log.Ltime)
	mcCli = conf.GetMC(config.MC.Addr)
	redisCli = conf.GetRedisCluster(config.RedisCluster.Addr)
	dbCli, err = conf.GetGorm(fmt.Sprintf(conf.MysqlAddr, config.Mysql.User, config.Mysql.Password, config.Mysql.Host, config.Mysql.Port, config.Mysql.DB))
	if err != nil {
		return err
	}
	slaveCli, err = conf.GetGorm(fmt.Sprintf(conf.MysqlAddr, config.Slave.User, config.Slave.Password, config.Slave.Host, config.Slave.Port, config.Slave.DB))
	return err
}

func (ss *SocialService) Follow(ctx context.Context, req *social_service.FollowRequest, res *social_service.EmptyResponse) error {
	switch req.FollowItem.FollowType {
	case constant.FollowTypePerson:
		return follow(ctx, req.FollowItem.Uid, req.FollowItem.TargetId)
	case constant.FollowTypeTopic:
		return followTopic(ctx, req.FollowItem.Uid, req.FollowItem.TargetId)
	default:
		return errors.New("parameter error")
	}
}

func (ss *SocialService) Unfollow(ctx context.Context, req *social_service.FollowRequest, res *social_service.EmptyResponse) error {
	switch req.FollowItem.FollowType {
	case constant.FollowTypePerson:
		return unfollow(ctx, req.FollowItem.Uid, req.FollowItem.TargetId)
	case constant.FollowTypeTopic:
		return unfollowTopic(ctx, req.FollowItem.Uid, req.FollowItem.TargetId)
	default:
		return errors.New("parameter error")
	}
}

func (ss *SocialService) GetFollow(ctx context.Context, req *social_service.ListRequest, res *social_service.ListResponse) error {
	var (
		ids     []int64
		hasMore bool
		err     error
	)
	switch req.FollowType {
	case constant.FollowTypePerson:
		ids, hasMore, err = getFollow(ctx, req.Uid, req.LastId, req.Offset)
	case constant.FollowTypeTopic:
		ids, hasMore, err = getFollowTopic(ctx, req.Uid, req.LastId, req.Offset)
	default:
		return errors.New("parameter error")
	}
	if err != nil {
		return err
	}
	res.Uids = ids
	res.HasMore = hasMore
	return nil
}

func (ss *SocialService) GetFollower(ctx context.Context, req *social_service.ListRequest, res *social_service.ListResponse) error {
	switch req.FollowType {
	case constant.FollowTypePerson:
		uids, hasMore, err := getFollower(ctx, req.Uid, req.LastId, req.Offset)
		if err != nil {
			return err
		}
		res.Uids = uids
		res.HasMore = hasMore
		return nil
	default:
		return errors.New("parameter error")
	}
}

func (ss *SocialService) GetFollowCount(ctx context.Context, req *social_service.CountRequest, res *social_service.CountResponse) error {
	var (
		followCnt   int64
		followerCnt int64
		err         error
	)

	switch req.FollowType {
	case constant.FollowTypePerson:
		followCnt, followerCnt, err = getFollowCount(ctx, req.Uid)
	case constant.FollowTypeTopic:
		followCnt, err = getFollowTopicCount(ctx, req.Uid)
	default:
		return errors.New("parameter error")
	}
	if err != nil {
		return err
	}
	res.FollowCount = followCnt
	res.FollowerCount = followerCnt
	return nil
}

func (ss *SocialService) GetFollowAll(ctx context.Context, res *social_service.FollowAllRequest, stream social_service.SocialServer_GetFollowAllStream) error {
	var (
		cursor uint64
		uid    int64
		uids   []int64
		err    error
	)
	uid = res.Uid
	for {
		uids, cursor, err = getAllFollow(ctx, uid, cursor)
		if err != nil {
			return err
		}
		err = stream.Send(&social_service.FollowAllResponse{Uids: uids})
		if err != nil {
			return err
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (ss *SocialService) GetFollowerAll(ctx context.Context, res *social_service.FollowAllRequest, stream social_service.SocialServer_GetFollowerAllStream) error {
	var (
		cursor uint64
		uid    int64
		uids   []int64
		err    error
	)
	uid = res.Uid
	for {
		uids, cursor, err = getAllFollower(ctx, uid, cursor)
		if err != nil {
			return err
		}
		err = stream.Send(&social_service.FollowAllResponse{Uids: uids})
		if err != nil {
			return err
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}
