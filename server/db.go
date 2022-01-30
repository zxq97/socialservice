package server

import (
	"context"
	"database/sql"
	"github.com/jinzhu/gorm"
	"socialservice/global"
	"time"
)

func dbFollow(ctx context.Context, uid, toUID int64) error {
	followItem := Follow{
		UID:       uid,
		FollowUID: toUID,
		Ctime:     time.Now(),
		Mtime:     time.Now(),
	}
	follower := Follower{
		UID:         toUID,
		FollowerUID: uid,
		Ctime:       time.Now(),
		Mtime:       time.Now(),
	}
	followCount := FollowCount{
		UID:           uid,
		FollowCount:   1,
		FollowerCount: 0,
	}
	followerCount := FollowCount{
		UID:           toUID,
		FollowCount:   0,
		FollowerCount: 1,
	}
	tx := dbCli.BeginTx(ctx, &sql.TxOptions{})
	defer tx.Rollback()
	err := tx.Create(followItem).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v add user_follow uid %v to_uid %v err %v", ctx, uid, toUID, err)
		return err
	}
	err = tx.Create(follower).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v add user_follower uid %v to_uid %v err %v", ctx, toUID, uid, err)
		return err
	}
	err = tx.Set("gorm:insert_option", "ON DUPLICATE key update follower_count = follower_count + 1").Create(&followCount).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v add user_follow_count uid %v err %v", ctx, uid, err)
		return err
	}
	err = tx.Set("gorm:insert_option", "ON DUPLICATE key update follower_count = follower_count + 1").Create(&followerCount).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v add user_follower_count uid %v err %v", ctx, toUID, err)
		return err
	}
	tx.Commit()
	return nil
}

func dbFollowTopic(ctx context.Context, uid, topicID int64) error {
	followTopicItem := FollowTopic{
		UID:     uid,
		TopicID: topicID,
		Ctime:   time.Now(),
		Mtime:   time.Now(),
	}
	followTopicCount := FollowTopicCount{
		UID:         uid,
		FollowCount: 1,
	}
	tx := dbCli.BeginTx(ctx, &sql.TxOptions{})
	defer tx.Rollback()
	err := dbCli.Create(&followTopicItem).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v create followtopic uid %v topicid %v err %v", ctx, uid, topicID, err)
		return err
	}
	err = tx.Set("gorm:insert_option", "ON DUPLICATE key update follow_count = follow_count + 1").Create(&followTopicCount).Error
	if err != nil {
		global.ExcLog.Printf("ctx add followtopiccnt uid %v err %v", uid, err)
		return err
	}
	tx.Commit()
	return nil
}

func dbUnfollow(ctx context.Context, uid, toUID int64) error {
	followCount := FollowCount{
		UID:           uid,
		FollowCount:   1,
		FollowerCount: 0,
	}
	followerCount := FollowCount{
		UID:           toUID,
		FollowCount:   0,
		FollowerCount: 1,
	}
	tx := dbCli.BeginTx(ctx, &sql.TxOptions{})
	defer tx.Rollback()
	err := dbCli.Where("uid = ? and follow_uid = ?", uid, toUID).Delete(&Follow{}).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v delete user_follow uid %v to_uid %v err %v", ctx, uid, toUID, err)
		return err
	}
	err = dbCli.Where("uid = ? and follower_uid = ?", toUID, uid).Delete(&Follower{}).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v delete user_follower uid %v to_uid %v err %v", ctx, toUID, uid, err)
		return err
	}
	err = dbCli.Model(&followCount).Where("uid = ? and follow_count > 0", uid).Update("follower_count", gorm.Expr("follow_count-1")).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v delete user_follow_count uid %v err %v", ctx, uid, err)
		return err
	}
	err = dbCli.Model(&followerCount).Where("uid = ? and follower_count > 0", toUID).Update("follower_counter", gorm.Expr("follower_count-1")).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v delete user_follower_count uid %v err %v", ctx, toUID, err)
		return err
	}
	tx.Commit()
	return nil
}

func dbUnfollowTopic(ctx context.Context, uid, topicID int64) error {
	followTopicCount := FollowTopicCount{
		UID:         uid,
		FollowCount: 1,
	}
	tx := dbCli.BeginTx(ctx, &sql.TxOptions{})
	defer tx.Rollback()
	err := dbCli.Where("uid = ? and topic_id = ?", uid, topicID).Delete(&FollowTopicCount{}).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v delete followtopic uid %v topicid %v err %v", ctx, uid, topicID, err)
		return err
	}
	err = dbCli.Model(&followTopicCount).Where("uid = ? and follow_count > 0", uid).Update("follow_count", gorm.Expr("follow_count - 1")).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v delete followtopiccount uid %v err %v", ctx, uid, err)
		return err
	}
	tx.Commit()
	return nil
}

func dbGetFollowCount(ctx context.Context, uid int64) (int64, int64, error) {
	followCount := FollowCount{}
	err := slaveCli.Select([]string{"follow_count", "follower_count"}).Where("uid = ?", uid).Find(&followCount).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v dbGetFollowCount uid %v err %v", ctx, uid, err)
		return 0, 0, err
	}
	return followCount.FollowCount, followCount.FollowerCount, nil
}

func dbGetFollowTopicCount(ctx context.Context, uid int64) (int64, error) {
	followTopicCount := FollowTopicCount{}
	err := slaveCli.Select("follow_count").Where("uid = ?", uid).Find(&followTopicCount).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v dbGetFollowTopicCount uid %v err %v", ctx, uid, err)
		return 0, err
	}
	return followTopicCount.FollowCount, nil
}

func dbGetFollow(ctx context.Context, uid int64) ([]int64, map[int64]int64, error) {
	follows := []Follow{}
	err := slaveCli.Select([]string{"follow_uid, ctime"}).Where("uid = ?", uid).Order("id desc").Find(&follows).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v db get user follow uid %v err %v", ctx, uid, err)
		return nil, nil, err
	}
	uids := make([]int64, 0, len(follows))
	followMap := make(map[int64]int64, len(follows))
	for _, v := range follows {
		uids = append(uids, v.FollowUID)
		followMap[v.FollowUID] = v.Ctime.Unix()
	}
	return uids, followMap, nil
}

func dbGetFollower(ctx context.Context, uid int64) ([]int64, map[int64]int64, error) {
	followers := []Follower{}
	err := slaveCli.Select([]string{"follower_uid, ctime"}).Where("uid = ?", uid).Order("id desc").Find(&followers).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v db get user follower uid %v err %v", ctx, uid, err)
		return nil, nil, err
	}
	uids := make([]int64, 0, len(followers))
	followMap := make(map[int64]int64, len(followers))
	for _, v := range followers {
		uids = append(uids, v.FollowerUID)
		followMap[v.FollowerUID] = v.Ctime.Unix()
	}
	return uids, followMap, nil
}

func dbGetFollowTopic(ctx context.Context, uid int64) ([]int64, map[int64]int64, error) {
	followTopics := []FollowTopic{}
	err := slaveCli.Select([]string{"topic_id, ctime"}).Where("uid = ?", uid).Order("id desc").Find(&followTopics).Error
	if err != nil {
		global.ExcLog.Printf("ctx %v dbGetFollowTopic uid %v err %v", ctx, uid, err)
		return nil, nil, err
	}
	topicIDs := make([]int64, 0, len(followTopics))
	topicMap := make(map[int64]int64, len(followTopics))
	for _, v := range followTopics {
		topicIDs = append(topicIDs, v.TopicID)
		topicMap[v.TopicID] = v.Ctime.Unix()
	}
	return topicIDs, topicMap, nil
}
