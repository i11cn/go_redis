package server

import (
	"bufio"
	"fmt"
	"github.com/i11cn/go_logger"
	"github.com/i11cn/go_redis/protocol"
	"reflect"
	"strings"
)

type (
	Handler interface {
		Serve(*bufio.Writer, string, []protocol.RESTPart) (bool, error)
	}
	HandleFunc func([]protocol.RESTPart) (*protocol.REST, error)

	CommonHandler struct {
		handle map[string]func([]protocol.RESTPart) (*protocol.REST, error)
		log    *logger.Logger
	}
)

func (ch *CommonHandler) Init(o interface{}) {
	t := reflect.TypeOf(o)
	fmt.Println(t.Name(), "共有", t.NumMethod(), "个方法")
	for i := 0; i < t.NumMethod(); i++ {
		fmt.Println(t.Method(i).Name)
	}
}

func (ch *CommonHandler) SetLogger(log *logger.Logger) {
	ch.log = log
}

func (ch *CommonHandler) Serve(w *bufio.Writer, cmd string, p []protocol.RESTPart) (bool, error) {
	if f, exist := ch.handle[cmd]; exist {
		if resp, err := f(p); err != nil {
			ch.log.Error(err.Error())
			if resp != nil {
				w.Write(protocol.EncodeREST(resp))
				w.Flush()
			}
			return false, err
		} else if resp != nil {
			_, err := w.Write(protocol.EncodeREST(resp))
			return true, err
		} else {
			return true, nil
		}
	}
	return false, nil
}

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
