package server

import (
	"bufio"
	"fmt"
	"go_redis/protocol"
	"net"
	"strings"
)

func init() {
}

type (
	CommandHandler interface {
		Serve(*bufio.Writer, *protocol.REST) (bool, error)
	}

	RedisServer struct {
		conn     net.Listener
		handlers []CommandHandler
		handle   map[string]func([]protocol.RESTPart) (*protocol.REST, error)
	}
)

func NewRedisServer() *RedisServer {
	return &RedisServer{handlers: make([]CommandHandler, 0), handle: make(map[string]func([]protocol.RESTPart) (*protocol.REST, error))}
}

func (s *RedisServer) Serve(w *bufio.Writer, r *protocol.REST) (bool, error) {
	if len(r.Parts) > 0 {
		cmd := string(r.Parts[0].Data)
		for _, h := range s.handlers {
			if p, err := h.Serve(w, r); p || err != nil {
				return true, err
			}
		}
		var resp *protocol.REST
		fmt.Println(protocol.DumpREST(resp))
		if resp == nil {
			var msg string
			switch r.Parts[0].Flag {
			case ':', '+', '-':
				msg = fmt.Sprintf("ERR unknown command '%s%s'", string(r.Parts[0].Flag), cmd)
			default:
				msg = fmt.Sprintf("ERR unknown command '%s'", cmd)
			}
			resp = &protocol.REST{false, msg, nil}
		}
		fmt.Println(protocol.DumpREST(resp))
		data := protocol.EncodeREST(resp)
		fmt.Println(data)
		_, err := w.Write(data)
		if err == nil {
			err = w.Flush()
		}
		return true, err
	}
	return true, nil
}

func (s *RedisServer) serve(conn net.Conn) {
	defer conn.Close()
	p := protocol.NewParser(conn)
	writer := bufio.NewWriter(conn)
	for {
		r, err := p.ReadREST()
		if err != nil {
			return
		}
		fmt.Println(protocol.DumpREST(r))
		p := false
		for _, h := range s.handlers {
			if p, err = h.Serve(writer, r); err != nil {
				return
			} else if p {
				break
			}
		}
		if !p {
			if _, err = s.Serve(writer, r); err != nil {
				return
			}
		}
	}
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
			go s.serve(conn)
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

func (s *RedisServer) HandleFunc(cmd string, f func([]protocol.RESTPart) (*protocol.REST, error)) {
	s.handle[strings.ToUpper(cmd)] = f
}
