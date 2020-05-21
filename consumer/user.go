package consumer

import (
	"encoding/json"
	"fmt"
	"goldserver/utils"
	"strconv"
	"strings"
	"time"
)

type UserDetal struct {
	Age        int    `json:"age"`
	Name       string `json:"name"`
	CreateTime string `json:"create_time"`
	Extra      string `json:"extra"`
}

type User struct {
	utils.Consumer
}

func (u User) String() string {
	return fmt.Sprintf("Age: %d, Name: %s, CreateTime: %s, Extra: %s", u.Age, u.Name, u.CreateTime, u.Extra)
}

func NewUser(c utils.Consumer) User {
	u := User{}
	u.Name = c.Name
	u.KafkaConfig = c.KafkaConfig
	u.MysqlConfig = c.MysqlConfig
	u.ConsumerConfig = c.ConsumerConfig
	u.Ready = make(chan bool)
	u.KafkaConn = utils.NewKafka(u.KafkaConfig)
	u.MysqlConn = utils.NewMysql(u.MysqlConfig)
	u.MsgChan = make(chan []byte, 600)
	u.ExitChan = make(chan int, 1)
	return u
}

func (u User) Handler() {
	tiker := time.NewTicker(time.Duration(u.ConsumerConfig.InsertFreq) * time.Millisecond)
	defer tiker.Stop()

	var msgArr = []UserDetal{}
	for {
		select {
		case <-u.ExitChan:
			time.Sleep(1 * time.Second)
			l := len(msgArr)
			utils.Log.Info(u.Name + " msgChanConsumer stop and the length of msgArr is " + strconv.Itoa(l))
			if l > 0 {
				u.BatchAdd(msgArr)
			}
			return
		case <-tiker.C:
			l := len(msgArr)
			if l >= u.ConsumerConfig.BatchAddNum {
				l = u.ConsumerConfig.BatchAddNum
			}
			if l > 0 {
				utils.Log.Info(u.Name + " msgChanConsumer runs regularly and the length of msgArr is " + strconv.Itoa(l))
				u.BatchAdd(msgArr[:l])
				msgArr = msgArr[l:]
			}
		case msg := <-u.MsgChan:
			var user UserDetal
			err := json.Unmarshal([]byte(msg), &goldDetail)
			if err == nil {
				msgArr = append(msgArr, user)
				l := len(msgArr)
				if l >= u.ConsumerConfig.BatchAddNum {
					utils.Log.Info(u.Name + " msgChanConsumer recives data from msgchannel and the length of msgArr is more than " + strconv.Itoa(u.ConsumerConfig.BatchAddNum))
					u.BatchAdd(msgArr[:u.ConsumerConfig.BatchAddNum])
					msgArr = msgArr[u.ConsumerConfig.BatchAddNum:]
				} else {
					utils.Log.Info(u.Name + " msgChanConsumer recives data from msgchannel and the length of msgArr is " + strconv.Itoa(l))
				}
			} else {
				utils.Log.Error(err.Error())
			}
		}
	}
}

func (u User) BatchAdd(userArr []UserDetal) {
	if len(userArr) <= 0 {
		return
	}

	l := len(userArr)
	valueStrings := make([]string, 0, l)
	valueArgs := make([]interface{}, 0, l*4)
	for _, user := range userArr {
		valueStrings = append(valueStrings, "(?,?,?,?)")
		valueArgs = append(valueArgs, user.Age, user.Name, user.CreateTime, user.Extra)
	}

	sqlPrefix := `INSERT IGNORE INTO %s (age,name,create_time,extra) VALUES %s`
	sql := fmt.Sprintf(u.Table, strings.Join(valueStrings, `,`))
	res, err := u.MysqlConn.Exec(sql, valueArgs...)
	if err != nil {
		utils.Log.Error(u.Name + ` batch insert to ` + u.Table + ` error ` + err.Error())
		return
	}
	id, insertErr := res.LastInsertId()
	if id > 0 {
		utils.Log.Info(u.Name + ` batch insert to ` + u.Table + ` succ, data length is ` + strconv.Itoa(l) + `, first insert id is ` + strconv.FormatInt(id, 10))
	} else if insertErr != nil {
		utils.Log.Error(u.Name + ` batch insert to ` + u.Table + ` error ` + insertErr.Error())
	} else {
		utils.Log.Info(u.Name + ` batch insert to ` + u.Table + ` detected duplicate data, data length is ` + strconv.Itoa(l))
	}
}
