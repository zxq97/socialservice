package generate

import (
	"github.com/bwmarrin/snowflake"
	"time"
)

var (
	node *snowflake.Node
	err  error
)

func InitSnowFlask() error {
	node, err = snowflake.NewNode(time.Now().UnixNano() % 1024)
	return err
}

func SnowFlask() int64 {
	return node.Generate().Int64()
}
