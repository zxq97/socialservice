package server

import "time"

type Follow struct {
	UID       int64     `json:"uid"`
	FollowUID int64     `json:"follow_uid"`
	Ctime     time.Time `json:"ctime"`
	Mtime     time.Time `json:"mtime"`
}

type Follower struct {
	UID         int64     `json:"uid"`
	FollowerUID int64     `json:"follower_uid"`
	Ctime       time.Time `json:"ctime"`
	Mtime       time.Time `json:"mtime"`
}

type FollowCount struct {
	UID           int64 `json:"uid"`
	FollowCount   int64 `json:"follow_count"`
	FollowerCount int64 `json:"follower_count"`
}

type FollowTopic struct {
	UID     int64     `json:"uid"`
	TopicID int64     `json:"topic_id"`
	Ctime   time.Time `json:"ctime"`
	Mtime   time.Time `json:"mtime"`
}

type FollowTopicCount struct {
	UID         int64 `json:"uid"`
	FollowCount int64 `json:"follow_count"`
}

func (t *Follow) TableName() string {
	return "follow"
}

func (t *Follower) TableName() string {
	return "follower"
}

func (t *FollowCount) TableName() string {
	return "follow_count"
}

func (t *FollowTopic) TableName() string {
	return "follow_topic"
}

func (t *FollowTopicCount) TableName() string {
	return "follow_topic_count"
}
