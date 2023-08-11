package kvdb

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type Client struct {
	host   string
	port   int
	conn   net.Conn
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func RunClient() {
	var (
		host string
		port int
	)
	flag.StringVar(&host, "host", "127.0.0.1", "set kvdb server host")
	flag.IntVar(&port, "port", 28999, "set kvdb server port")
	flag.Parse()
	printUsage()
	client, err := newClient(host, port)
	if err != nil {
		fmt.Println("  connect server failed: ", err)
		return
	}
	fmt.Println("  connect server successfully")
	client.wg.Wait()
	fmt.Println("client close")
}

func newClient(host string, port int) (*Client, error) { //new完必须stop
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		host:   host,
		port:   port,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
	}
	client.wg.Add(2)
	go client.handleConn()
	go client.heartBeat()
	return client, nil
}

func (c *Client) handleConn() {
	defer c.wg.Done()
	defer c.conn.Close()
	buf := make([]byte, 1024)
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-c.ctx.Done():
			fmt.Println("stop handle connect")
			return
		default:
			fmt.Print(fmt.Sprintf("kvdb %v:%v>", c.host, c.port))
			command, err := reader.ReadString('\n') //好像能换成io.copy
			if err != nil {
				if err != io.EOF {
					fmt.Println("read from console failed: ", err)
				}
				continue
			}
			command = strings.TrimSpace(command)
			if command == "" { //改成switch
				continue
			}
			if command == "clear" {
				cmd := exec.Command("cmd", "/C", "cls")
				cmd.Stdout = os.Stdout
				err = cmd.Run()
				if err != nil {
					fmt.Println("clear console failed: ", err)
				}
				continue
			}
			if command == "exit" {
				fmt.Println("stop handle connect")
				c.cancel()
				return
			}
			if command == "stop" {
				fmt.Println("stop server")
				fmt.Println("stop handle connect")
				c.conn.Write([]byte("stop"))
				c.cancel()
				return
			}
			_, err = c.conn.Write([]byte(command))
			if err != nil {
				fmt.Println("write to connect failed: ", err, ", please wait seconds")
				err = c.tryReConnect()
				if err != nil {
					log.Println(err)
					return
				}
				continue
			}
			n, err := c.conn.Read(buf)
			if err != nil {
				fmt.Println("read from connect failed: ", err, ", please wait seconds")
				err = c.tryReConnect()
				if err != nil {
					log.Println(err)
					return
				}
				continue
			}
			fmt.Println(string(buf[:n]))
		}
	}
}

func (c *Client) heartBeat() {
	defer c.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			fmt.Println("stop heartbeat")
			return
		case <-ticker.C:
			_, err := c.conn.Write([]byte{2, 0, 0, 1})
			if err != nil {
				fmt.Println("\nheartbeat failed: ", err, ", please wait seconds")
				err = c.tryReConnect()
				if err != nil {
					fmt.Println(err)
					c.cancel()
					return
				}
			}
		}
	}
}

func (c *Client) tryReConnect() error {
	for i := 0; i < 5; i++ {
		fmt.Println("try reconnect")
		var err error
		c.conn, err = net.Dial("tcp", fmt.Sprintf("%v:%v", c.host, c.port))
		if err == nil {
			fmt.Println("reconnect successfully")
			fmt.Print(fmt.Sprintf("kvdb %v:%v>", c.host, c.port))
			return nil
		}
		fmt.Printf("reconnect failed: %v\n", err)
	}
	return errors.New("all 5 time reconnect failed, please check your network or maybe server shutdown and press ctrl+c to stop client")
}
