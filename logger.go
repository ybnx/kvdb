package kvdb

import (
	"errors"
	"fmt"
	"path"
	"runtime"
	"strings"
	"time"
)

type LogLevel uint8

type Method func(string)

type Logger struct {
	level   LogLevel
	method  Method
	logChan chan *LogMsg
}

type LogMsg struct {
	timestamp, levelStr, fileName, funcName, msg string
	lineNum                                      int
}

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

func NewLogger(level LogLevel, method Method) (logger *Logger, err error) {
	if level > FATAL {
		err = errors.New("unknown level")
	}
	logger = &Logger{
		level:   level,
		method:  method,
		logChan: make(chan *LogMsg, 500),
	}
	go workBackGround(logger)
	return
}

func (logger Logger) Debug(format string, a ...interface{}) {
	logTo(format, "DEBUG", logger, DEBUG, a...)
}

func (logger Logger) Info(format string, a ...interface{}) {
	logTo(format, "INFO", logger, INFO, a...)
}

func (logger Logger) Warning(format string, a ...interface{}) {
	logTo(format, "WARNING", logger, WARNING, a...)
}

func (logger Logger) Error(format string, a ...interface{}) {
	logTo(format, "ERROR", logger, ERROR, a...)
}

func (logger Logger) Fatal(format string, a ...interface{}) {
	logTo(format, "FATAL", logger, FATAL, a...)
}

func logTo(format string, levelStr string, logger Logger, level LogLevel, a ...interface{}) {
	if level >= logger.level {
		msg := fmt.Sprintf(format, a...)
		now := time.Now()
		funcName, fileName, lineNum := getInfo(3)
		logTmp := &LogMsg{
			timestamp: now.Format("2006-01-02 15:04:05"),
			levelStr:  levelStr,
			fileName:  fileName,
			funcName:  funcName,
			msg:       msg,
			lineNum:   lineNum,
		}
		select {
		case logger.logChan <- logTmp:
		default: //健壮性：如果通道满了，就只能把这条日志抛弃掉，保证业务代码不出现阻塞
		}
	}
	return
}

func getInfo(skip int) (funcName, fileName string, lineNum int) {
	pc, file, lineNum, ok := runtime.Caller(skip)
	if !ok {
		fmt.Println("runtime.Caller error")
		return
	}
	funcName = runtime.FuncForPC(pc).Name()
	funcName = strings.Split(funcName, ".")[1]
	fileName = path.Base(file) //通过绝对路径获取文件名main.go
	return
}

func workBackGround(logger *Logger) {
	for {
		select {
		case logTmp := <-logger.logChan:
			output := fmt.Sprintf("[%s] [%s] [%s:%s:%d] %s\n", logTmp.timestamp, logTmp.levelStr, logTmp.fileName, logTmp.funcName, logTmp.lineNum, logTmp.msg)
			logger.method(output)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
