package server

import (
	"bufio"
	"errors"
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

func NewCommonHandler(o interface{}) *CommonHandler {
	ret := &CommonHandler{log: logger.GetLogger("redis")}
	ret.Handle(o)
	return ret
}

func (ch *CommonHandler) SetLogger(log *logger.Logger) {
	ch.log = log
}

func (ch *CommonHandler) Handle(o interface{}) {
	ch.handle = make(map[string]func([]protocol.RESTPart) (*protocol.REST, error))
	method_names := make([]string, 0)
	t := reflect.TypeOf(o)
	for i := 0; i < t.NumMethod(); i++ {
		if len(t.Method(i).PkgPath) == 0 {
			method_names = append(method_names, t.Method(i).Name)
		}
	}
	v := reflect.ValueOf(o)
	for _, n := range method_names {
		var f func([]protocol.RESTPart) (*protocol.REST, error)
		reflect.ValueOf(&f).Elem().Set(v.MethodByName(n))
		ch.handle[strings.ToUpper(n)] = f
		ch.log.Trace("注册了指令: ", strings.ToUpper(n))
	}
}

func (ch *CommonHandler) Serve(w *bufio.Writer, cmd string, p []protocol.RESTPart) (bool, error) {
	ch.log.Trace("准备处理指令 - ", cmd)
	if f, exist := ch.handle[cmd]; exist {
		for _, a := range p {
			if a.Flag != '$' {
				resp := NewErrorREST("Protocol error: expected '$', got '", string(a.Flag), "'")
				w.Write(protocol.EncodeRespREST(resp))
				w.Flush()
				return true, errors.New("命令格式错误")
			}

		}
		if resp, err := f(p); err != nil {
			ch.log.Trace("获得响应:", resp, ", err:", err)
			ch.log.Error(err.Error())
			if resp != nil {
				w.Write(protocol.EncodeRespREST(resp))
				w.Flush()
			}
			return false, err
		} else if resp != nil {
			ch.log.Trace("获得响应:", resp, ", err:", err)
			_, err := w.Write(protocol.EncodeRespREST(resp))
			return true, err
		} else {
			ch.log.Trace("获得响应:", resp, ", err:", err)
			return true, nil
		}
	}
	return false, nil
}

func NewREST(datas ...interface{}) *protocol.REST {
	ret := &protocol.REST{true, "OK", make([]protocol.RESTPart, 0, len(datas))}
	for _, d := range datas {
		if d == nil {
			ret.Parts = append(ret.Parts, protocol.RESTPart{'$', nil, 0})
		} else {
			switch o := d.(type) {
			case int, int8, int16, int32, int64:
				ret.Parts = append(ret.Parts, protocol.RESTPart{':', []byte{}, int(reflect.ValueOf(o).Int())})
			case uint, uint8, uint16, uint32, uint64:
				ret.Parts = append(ret.Parts, protocol.RESTPart{':', []byte{}, int(reflect.ValueOf(o).Uint())})
			case []byte:
				ret.Parts = append(ret.Parts, protocol.RESTPart{'$', o, len(o)})
			case string:
				ret.Parts = append(ret.Parts, protocol.RESTPart{'$', []byte(o), len(o)})
			}
		}
	}
	return ret
}

func NewErrorREST(msgs ...string) *protocol.REST {
	msg := fmt.Sprint("ERR ", strings.Join(msgs, ""))
	return &protocol.REST{false, msg, []protocol.RESTPart{}}
}
