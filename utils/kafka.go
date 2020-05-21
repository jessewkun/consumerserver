package utils

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

type KafkaConfig struct {
	Group   string   `yaml:"group"`
	Brokers []string `yaml:"brokers"`
	Topics  []string `yaml:"topics"`
}

func NewKafka(kc KafkaConfig) sarama.ConsumerGroup {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_2
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup(kc.Brokers, kc.Group, config)
	if err != nil {
		Log.Error("Error creating consumer group client: " + err.Error())
		panic(err)
	}
	logstr := fmt.Sprintf("NewKafka: brokers %s, topics: %s, group: %s", strings.Join(kc.Brokers, ";"), strings.Join(kc.Topics, ";"), kc.Group)
	Log.Info(logstr)
	return client
}
