package kvdb

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	LOGFILE     = "E:\\golangProject\\demo0\\osfile\\logger.log"
	ERRORFILE   = "E:\\golangProject\\demo0\\osfile\\error.log"
	MAXFILESIZE = 1024
)

func Console(output string) {
	fmt.Print(output)
}

func File(output string) {
	fileOut(LOGFILE, output)
}

func ErrorFile(output string) {
	fileOut(ERRORFILE, output)
}

func fileOut(fileName, output string) {
	fileObj, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer fileObj.Close()
	if checkSize(fileObj) {
		fileObj = splitFile(fileObj, fileName)
		defer fileObj.Close()
	}
	fileObj.WriteString(output)
}

// 判断是否切割文件
func checkSize(file *os.File) bool {
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("get file info failed: %v\n", err)
		return false
	}
	return fileInfo.Size() >= MAXFILESIZE
}

// 切割文件
func splitFile(file *os.File, fileName string) *os.File {
	//关闭当前日志文件
	file.Close()
	//重命名旧的日志文件
	filePath := strings.Split(fileName, ".")[0]
	nowStr := time.Now().Format("20060102150405")
	newLogName := fmt.Sprintf("%s.%s.log", filePath, nowStr)
	os.Rename(fileName, newLogName)
	//打开新的日志文件
	fileObj, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("open file failed: %v\n", err)
		panic(err)
	}
	//把新的日志文件对象赋值给fileObj
	return fileObj
}
