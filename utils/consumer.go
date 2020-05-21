package utils

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type ConsumerConfig struct {
	BatchAddNum int `yml:"batchaddnum"` // 批量写入个数
	KafkaFreq   int `yml:"kafkafreq"`   // kafka读取数据频率
	InsertFreq  int `yml:"insertfreq"`  // 数据定时入库频率
}

type Consumer struct {
	Name           string         `yaml:"name"`
	KafkaConfig    KafkaConfig    `yaml:"kafkaconfig"`
	MysqlConfig    MysqlConfig    `yaml:"mysqlconfig"`
	ConsumerConfig ConsumerConfig `yaml:"consumerconfig"`
	Ready          chan bool
	KafkaConn      sarama.ConsumerGroup
	MysqlConn      *sql.DB
	MsgChan        chan []byte
	ExitChan       chan int
}

type ConsumerInterfacer interface {
	Consume(ctx context.Context, wg *sync.WaitGroup)
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
	Handler()
	CloseDb()
	CloseKafKa()
	Iready()
}

func (c Consumer) Consume(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if err := c.KafkaConn.Consume(ctx, c.KafkaConfig.Topics, c); err != nil {
			Log.Error(c.Name + " Error from consumer: " + err.Error())
		}
		if ctx.Err() != nil {
			close(c.ExitChan)
			Log.Info(c.Name + " consumer Exit")
			return
		}
		c.Ready = make(chan bool)
	}
}

func (c Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.Ready)
	return nil
}

func (c Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		logstr := fmt.Sprintf("%s/%d/%d\t%s", message.Topic, message.Partition, message.Offset, message.Value)
		Log.Info(logstr)
		c.MsgChan <- message.Value
		session.MarkMessage(message, "")
		if c.ConsumerConfig.KafkaFreq > 0 {
			time.Sleep(time.Duration(c.ConsumerConfig.KafkaFreq) * time.Millisecond)
		}
	}
	return nil
}

func (c Consumer) Handler() {
	fmt.Printf("%+v\n", "------")
	fmt.Printf("%+v\n", string(<-c.MsgChan))
	fmt.Printf("%+v\n", "------")
}

func (c Consumer) CloseDb() {
	if c.MysqlConn != nil {
		if err := c.MysqlConn.Close(); err != nil {
			panic(err)
		}
		logstr := fmt.Sprintf(c.Name+" close mysql conn %s@%s:%s", c.MysqlConfig.User, c.MysqlConfig.Host, c.MysqlConfig.Db)
		Log.Info(logstr)
	}
}

func (c Consumer) CloseKafKa() {
	if c.KafkaConn != nil {
		if err := c.KafkaConn.Close(); err != nil {
			panic(err)
		}
		logstr := fmt.Sprintf(c.Name+" close kafka conn: brokers %s, topics: %s, group: %s", strings.Join(c.KafkaConfig.Brokers, ";"), strings.Join(c.KafkaConfig.Topics, ";"), c.KafkaConfig.Group)
		Log.Info(logstr)
	}
}

func (c Consumer) Iready() {
	<-c.Ready
}
