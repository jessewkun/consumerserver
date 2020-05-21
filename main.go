package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"consumerserver/consumer"
	"consumerserver/utils"

	"github.com/jinzhu/configor"
)

var configfile string
var config Config

type Config struct {
	User utils.Consumer `yaml:"user"`
	// 在这里添加新的即可
	Env string `yaml:"env"`
}

func initFlag() {
	flag.StringVar(&configfile, "c", "config.yml", "config file path")
	flag.Parse()
}

func main() {
	initFlag()
	if err := configor.Load(&config, configfile); err != nil {
		panic(err)
	}
	config.Log.Env = config.Env
	utils.NewLogger(config.Log)
	utils.Log.Info("ConsumerServer start")

	AllConsumers := []utils.ConsumerInterfacer{
		consumer.NewUser(config.User),
		// 在这里添加新的即可
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(len(AllConsumers))
	for _, item := range AllConsumers {
		go item.Handler()
		time.Sleep(time.Second)
		go item.Consume(ctx, wg)
		item.Iready()
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		utils.Log.Info("ConsumerServer context cancelled")
	case s := <-sigterm:
		utils.Log.Info("ConsumerServer catch signal " + s.String())
	}
	cancel()
	wg.Wait()

	time.Sleep(3 * time.Second)

	for _, item := range AllConsumers {
		item.CloseDb()
		item.CloseKafKa()
	}

	utils.Log.Info("ConsumerServer stop")
}
