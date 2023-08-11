package kvdb

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type Server struct {
	db       *DB
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	listener net.Listener
	stopFlag chan struct{}
	host     string
	port     int
}

func RunServer() {
	var (
		host           string
		port           int
		maxMemoryUsage int
		dbPath         string
	)
	flag.StringVar(&host, "host", "127.0.0.1", "set kvdb server host")
	flag.IntVar(&port, "port", 28999, "set kvdb server port")
	flag.IntVar(&maxMemoryUsage, "max", 600*MIB, "set kvdb max memory usage")
	flag.StringVar(&dbPath, "path", "E:\\golangProject\\demo2\\dbtest\\db.txt", "set kbdb database file path")
	flag.Parse()
	opt := &Option{
		MaxMemoryUsage: int64(maxMemoryUsage),
		DbPath:         dbPath,
	}
	printUsage()
	server, err := NewServer(host, port, opt) //new完必须close
	if err != nil {
		log.Println("kvdb: new server failed: ", err)
		return
	}
	err = server.Listen()
	if err != nil {
		log.Println("kvdb: listen connect failed: ", err)
		return
	}
	log.Printf("kvdb: server listen on host %v and port %v\n", host, port)
	server.wg.Wait()
	server.db.Close()
	log.Println("kvdb: server shutdown")
}

func NewServer(host string, port int, dbOpt *Option) (*Server, error) { //如果使用NewServer开启服务，请不要用客户端stop命令结束服务端，必须使用Stop方法
	db, err := Open(dbOpt)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{
		db:       db,
		ctx:      ctx,
		cancel:   cancel,
		stopFlag: make(chan struct{}, 1),
		host:     host,
		port:     port,
	}
	return server, nil
}

func (s *Server) Listen() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", s.host, s.port))
	if err != nil {
		return err
	}
	s.listener = listener
	s.wg.Add(1)
	go s.createConn()
	return nil
}

func (s *Server) createConn() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopFlag:
				log.Println("kvdb: server stop listener")
				return
			default:
				log.Println("kvdb: server accept listener failed: ", err)
			}
			continue
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	buf := make([]byte, 1024)
	s.db.Update(func(tx *TX) error {
		var err error
		for {
			select {
			case <-s.ctx.Done():
				s.stop()
				return nil
			default:
				var n int
				n, err = conn.Read(buf)
				if err != nil {
					if err == io.EOF { //为什么client exit这里会eof
						return nil
					}
					log.Println("kvdb: read from connect failed: ", err)
					return nil
				}
				if bytes.Equal(buf[:n], []byte{2, 0, 0, 1}) { //heartbeat跳过
					continue
				}
				cmd := string(buf[:n])
				if cmd == "stop" {
					s.stop()
					return nil
				}
				if cmd == "info" {
					_, err = conn.Write([]byte(fmt.Sprintf("maxMemoryUsage: %v\ndbPath: %v", s.db.option.MaxMemoryUsage, s.db.option.DbPath)))
					if err != nil {
						log.Println("kvdb: write to connect failed: ", err)
						continue
					}
				}
				var res string
				res = s.handleCmd(tx, cmd)
				_, err = conn.Write([]byte(res))
				if err != nil {
					log.Println("kvdb: write to connect failed: ", err)
					return nil
				}
			}
		}
	})
	log.Println("kvdb: server close connection", conn.RemoteAddr().String())
}

func (s *Server) handleCmd(tx *TX, cmd string) string {
	args := strings.Fields(cmd)
	handler := SupportedCommand[args[0]]
	if handler == nil {
		return "unsupported command"
	}
	return handler(tx, args[1:])
}

func (s *Server) Stop() {
	s.stop()
	log.Println("kvdb: server shutdown")
}

func (s *Server) stop() { //命令行时不调用
	log.Println("kvdb: stop handle connect")
	s.stopFlag <- struct{}{}
	s.listener.Close()
	s.cancel() //关闭所有连接的groutine
}

func printUsage() {
	fmt.Println("  _                  _   _     \n | |                | | | |    \n | | __ __   __   __| | | |__  \n | |/ / \\ \\ / /  / _` | | '_ \\ \n |   <   \\ V /  | (_| | | |_) |\n |_|\\_\\   \\_/    \\__,_| |_.__/ \n                               ")
	fmt.Println("  A kv database which support concurrent transaction")
	fmt.Println("  select [bucket]            ------> select bucket")
	fmt.Println("  put [key] [value]          ------> put key value")
	fmt.Println("  putTTL [key] [value] [ttl] ------> put key value with ttl")
	fmt.Println("  get [key]                  ------> get key value")
	fmt.Println("  delete [key]               ------> delete key value")
	fmt.Println("  getDelete [key]            ------> get and delete key value")
	fmt.Println("  range [start] [end]        ------> range select")
	fmt.Println("  info                       ------> get kvdb server info")
	fmt.Println("  clear                      ------> clear screen")
	fmt.Println("  exit                       ------> exit kvdb client")
	fmt.Println("  stop                       ------> stop kvdb server and exit kvdb client")
}
