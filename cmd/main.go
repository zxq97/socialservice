package main

import (
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/registry/etcd"
	"socialservice/conf"
	"socialservice/global"
	"socialservice/rpc/social/pb"
	"socialservice/server"
)

var (
	socialConf *conf.Conf
	err        error
)

func main() {
	socialConf, err = conf.LoadYaml(conf.SocialConfPath)
	if err != nil {
		panic(err)
	}

	global.InfoLog, err = conf.InitLog(socialConf.LogPath.Info)
	if err != nil {
		panic(err)
	}
	global.ExcLog, err = conf.InitLog(socialConf.LogPath.Exc)
	if err != nil {
		panic(err)
	}
	global.DebugLog, err = conf.InitLog(socialConf.LogPath.Debug)
	if err != nil {
		panic(err)
	}

	err = server.InitService(socialConf)
	if err != nil {
		panic(err)
	}

	etcdRegistry := etcd.NewRegistry(func(options *registry.Options) {
		options.Addrs = socialConf.Etcd.Addr
	})

	service := micro.NewService(
		micro.Name(socialConf.Grpc.Name),
		micro.Address(socialConf.Grpc.Addr),
		micro.Registry(etcdRegistry),
	)
	service.Init()
	err = social_service.RegisterSocialServerHandler(
		service.Server(),
		new(server.SocialService),
	)
	if err != nil {
		panic(err)
	}
	err = service.Run()
	if err != nil {
		panic(err)
	}
}
