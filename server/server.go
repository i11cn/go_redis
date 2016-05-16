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
		SetHandle(cmd string, f func([]protocol.RESTPart) (*protocol.REST, error))
		RemoveHandle(cmd string)
		GetHandle(cmd string) func([]protocol.RESTPart) (*protocol.REST, error)
	}

	RedisServer struct {
		conn     net.Listener
		handlers []CommandHandler
	}

	default_handler struct {
		handle map[string]func([]protocol.RESTPart) (*protocol.REST, error)
	}
)

func (d *default_handler) SetHandle(cmd string, f func([]protocol.RESTPart) (*protocol.REST, error)) {
	if d.handle == nil {
		d.handle = make(map[string]func([]protocol.RESTPart) (*protocol.REST, error))
	}
	d.handle[cmd] = f
}

func (d *default_handler) RemoveHandle(cmd string) {
	if d.handle != nil {
		delete(d.handle, cmd)
	}
}

func (d *default_handler) GetHandle(cmd string) func([]protocol.RESTPart) (*protocol.REST, error) {
	if d.handle != nil {
		if ret, exist := d.handle[cmd]; exist {
			return ret
		}
	}
	return nil
}

func NewRedisServer() *RedisServer {
	return &RedisServer{handlers: []CommandHandler{&default_handler{}}}
}

func (s *RedisServer) process_rest(w *bufio.Writer, r *protocol.REST) error {
	fmt.Println(protocol.DumpREST(r))
	if len(r.Parts) > 0 {
		cmd := string(r.Parts[0].Data)
		var resp *protocol.REST
		for _, h := range s.handlers {
			var err error
			if f := h.GetHandle(strings.ToUpper(cmd)); f != nil {
				resp, err = f(r.Parts[1:])
				if err != nil {
					return err
				}
				break
			}
		}
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
		return err
	}
	return nil
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
		if err = s.process_rest(writer, r); err != nil {
			return
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
	s.handlers[len(s.handlers)-1].SetHandle(strings.ToUpper(cmd), f)
}
