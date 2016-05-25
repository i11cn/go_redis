package server

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/i11cn/go_redis/protocol"
	"net"
	"reflect"
	"strings"
)

func init() {
}

type (
	CommandHandler interface {
		Serve(*bufio.Writer, string, []protocol.RESTPart) (bool, error)
	}
	HandleFunc func([]protocol.RESTPart) (*protocol.REST, error)

	RedisServer struct {
		conn     net.Listener
		handlers []CommandHandler
		handle   map[string]HandleFunc
	}
)

func NewREST(datas ...interface{}) *protocol.REST {
	ret := &protocol.REST{true, "", make([]protocol.RESTPart, 0, len(datas))}
	for _, d := range datas {
		switch o := d.(type) {
		case int, int8, int16, int32, int64:
			ret.Parts = append(ret.Parts, protocol.RESTPart{':', []byte{}, int(reflect.ValueOf(o).Int())})
		case uint, uint8, uint16, uint32, uint64:
			ret.Parts = append(ret.Parts, protocol.RESTPart{':', []byte{}, int(reflect.ValueOf(o).Uint())})
		case []byte:
			ret.Parts = append(ret.Parts, protocol.RESTPart{'$', o, len(o)})
		}
	}
	return ret
}

func NewErrorREST(msgs ...string) *protocol.REST {
	msg := fmt.Sprint("ERR ", strings.Join(msgs, ""))
	return &protocol.REST{false, msg, []protocol.RESTPart{}}
}

func NewRedisServer() *RedisServer {
	ret := &RedisServer{handlers: make([]CommandHandler, 0), handle: make(map[string]HandleFunc)}
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

func (s *RedisServer) Handle(h CommandHandler) {
	tmp := make([]CommandHandler, 0, len(s.handlers)+1)
	tmp = append(tmp, h)
	tmp = append(tmp, s.handlers...)
	s.handlers = tmp
}

func (s *RedisServer) HandleFunc(cmd string, f HandleFunc) {
	s.handle[strings.ToUpper(cmd)] = f
}

func (s *RedisServer) Serve(w *bufio.Writer, cmd string, p []protocol.RESTPart) (bool, error) {
	if f, exist := s.handle[cmd]; exist {
		if resp, err := f(p); err != nil {
			return false, err
		} else {
			_, err := w.Write(protocol.EncodeREST(resp))
			return true, err
		}
	}
	return false, nil
}

func (s *RedisServer) call_handlers(w *bufio.Writer, cmd string, p []protocol.RESTPart) (bool, error) {
	for _, h := range s.handlers {
		if p, err := h.Serve(w, cmd, p); p || err != nil {
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
					if _, err := writer.Write(protocol.EncodeREST(resp)); err != nil {
						return
					}
				}
			}
			writer.Flush()
		}
	}
}
