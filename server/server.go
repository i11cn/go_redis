package server

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/i11cn/go_logger"
	"github.com/i11cn/go_redis/protocol"
	"net"
	"strings"
	"time"
)

func init() {
}

type (
	RedisServer struct {
		conn     net.Listener
		handlers []Handler
		handle   map[string]HandleFunc
		log      *logger.Logger
	}
)

func init() {
	logger.GetLogger("redis").AddAppender(logger.NewConsoleAppender("%T [%N] %L - %M"))
	logger.GetLogger("redis").AddAppender(logger.NewSplittedFileAppender("%T [%N] %L - %M", "redis.log", 24*time.Hour))
}

func NewRedisServer() *RedisServer {
	ret := &RedisServer{handlers: make([]Handler, 0), handle: make(map[string]HandleFunc), log: logger.GetLogger("redis")}
	ret.log.Trace("创建 Redis 服务器")
	ret.HandleFunc("quit", func([]protocol.RESTPart) (*protocol.REST, error) {
		return nil, errors.New("客户端主动关闭连接")
	})
	return ret
}

func (s *RedisServer) Start(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	s.conn = ln
	for {
		conn, err := s.conn.Accept()
		if err == nil {
			go s.client_routine(conn)
		}
	}
	return nil
}

func (s *RedisServer) Handle(o interface{}) {
	s.log.Trace("准备注册对象 ", o)
	if o == nil {
		return
	}
	if h, ok := o.(Handler); ok {
		s.handlers = append(s.handlers, h)
	} else {
		h := NewCommonHandler(o)
		s.handlers = append(s.handlers, h)
	}
}

func (s *RedisServer) HandleFunc(cmd string, f HandleFunc) {
	s.handle[strings.ToUpper(cmd)] = f
}

func (s *RedisServer) Serve(w *bufio.Writer, cmd string, p []protocol.RESTPart) (bool, error) {
	if f, exist := s.handle[cmd]; exist {
		if resp, err := f(p); err != nil {
			return false, err
		} else {
			_, err := w.Write(protocol.EncodeRespREST(resp))
			return true, err
		}
	}
	return false, nil
}

func (s *RedisServer) call_handlers(w *bufio.Writer, cmd string, p []protocol.RESTPart) (bool, error) {
	for i := len(s.handlers) - 1; i >= 0; i-- {
		if p, err := s.handlers[i].Serve(w, cmd, p); p || err != nil {
			return p, err
		}
	}
	return false, nil
}

func (s *RedisServer) client_routine(conn net.Conn) {
	defer conn.Close()
	p := protocol.NewParser(conn)
	writer := bufio.NewWriter(conn)
	for {
		r, err := p.ReadREST()
		if err != nil {
			return
		}
		if len(r.Parts) > 0 {
			cmd := string(r.Parts[0].Data)
			if p, err := s.call_handlers(writer, strings.ToUpper(cmd), r.Parts[1:]); err != nil {
				return
			} else if !p {
				if p, err = s.Serve(writer, strings.ToUpper(cmd), r.Parts[1:]); err != nil {
					return
				} else if !p {
					var msg string
					switch r.Parts[0].Flag {
					case ':', '+', '-':
						msg = fmt.Sprintf("ERR unknown command '%s%s'", string(r.Parts[0].Flag), cmd)
					default:
						msg = fmt.Sprintf("ERR unknown command '%s'", cmd)
					}
					resp := &protocol.REST{false, msg, nil}
					if _, err := writer.Write(protocol.EncodeRespREST(resp)); err != nil {
						return
					}
				}
			}
			writer.Flush()
		}
	}
}
