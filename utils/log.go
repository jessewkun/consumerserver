package utils

import (
	"fmt"
	"time"
)

var Log Logger

type Logger struct {
	Env string
}

func NewLogger(l Logger) {
	Log = Logger{
		Env: l.Env,
	}
}

func (l Logger) Error(logstr string) {
	s := fmt.Sprintf(`[`+l.Env+`][ERROR] %s %s`, Now(), logstr)
	fmt.Println(s)
}

func (l Logger) Info(logstr string) {
	s := fmt.Sprintf(`[`+l.Env+`][INFO] %s %s`, Now(), logstr)
	fmt.Println(s)
}

func Now() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
