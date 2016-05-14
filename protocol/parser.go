package protocol

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
)

type (
	Request struct {
		Command string
		Args    [][]byte
	}
	Response struct {
		Success bool
		Message string
		Data    []byte
		Length  int
	}
	RedisCommand struct {
		Request
		Response
	}

	Parser struct {
		rw     *bufio.ReadWriter
		cmd    chan RedisCommand
		server bool
	}
)

func EncodeRequest(r Request) []byte {
	var d bytes.Buffer
	if len(r.Command) > 0 {
		total := len(r.Args)
		d.WriteByte('*')
		d.WriteString(strconv.Itoa(total + 1))
		d.WriteString("\r\n")
		d.WriteString(strings.ToUpper(r.Command))
		d.WriteString("\r\n")
		for _, b := range r.Args {
			d.WriteByte('$')
			d.WriteString(strconv.Itoa(len(b)))
			d.WriteString("\r\n")
			d.Write(b)
			d.WriteString("\r\n")
		}
	}
	return d.Bytes()
}

func EncodeResponse(r Response) []byte {
	var d bytes.Buffer
	if r.Success {
		if len(r.Data) > 0 {
			d.WriteByte('$')
			d.WriteString(strconv.Itoa(len(r.Data)))
			d.WriteString("\r\n")
			d.Write(r.Data)
		} else if len(r.Message) > 0 {
			d.WriteByte('+')
			d.WriteString(r.Message)
		} else {
			d.WriteByte(':')
			d.WriteString(strconv.Itoa(r.Length))
		}
	} else {
		d.WriteByte('-')
		d.WriteString(r.Message)
	}
	d.WriteString("\r\n")
	return d.Bytes()
}

func NewParser(r io.Reader, w io.Writer) *Parser {
	ret := &Parser{}
	ret.rw = bufio.NewReadWriter(bufio.NewReaderSize(r, 65536), bufio.NewWriterSize(w, 65536))
	ret.cmd = make(chan RedisCommand, 1)
	return nil
}

func (p *Parser) AsServer() {
	p.server = true
}

func (p *Parser) AsClient() {
	p.server = false
}

func (p *Parser) GetMonitor() <-chan RedisCommand {
	return p.cmd
}

func (p *Parser) SendRequest(r Request) error {
	_, err := p.rw.Write(EncodeRequest(r))
	if err == nil {
		err = p.rw.Flush()
	}
	return err
}

func (p *Parser) SendResponse(r Response) error {
	_, err := p.rw.Write(EncodeResponse(r))
	if err == nil {
		err = p.rw.Flush()
	}
	return err
}

func (p *Parser) fail(bs ...[]byte) error {
	var buf bytes.Buffer
	for _, b := range bs {
		buf.Write(b)
	}
	resp := Response{}
	resp.Success = false
	resp.Message = buf.String()
	return p.SendResponse(resp)
}

func (p *Parser) get_flag() (byte, error) {
	b, err := p.rw.ReadByte()
	if err != nil {
		return 0, err
	}
	return b, nil
}

func (p *Parser) get_int() (int, error) {
	d, _, err := p.rw.ReadLine()
	if err != nil {
		return 0, err
	}
	ret, err := strconv.ParseInt(string(d), 10, 32)
	if err != nil {
		return 0, err
	}
	return int(ret), nil
}

func (p *Parser) get_string() (string, error) {
	d, _, err := p.rw.ReadLine()
	if err != nil {
		return "", err
	}
	return string(d), nil
}

func (p *Parser) get_data(l int) ([]byte, error) {
	d := make([]byte, l+2)
	pos := 0
	var err error = nil
	var n int = 0
	for ; err == nil && n < (l-pos); n, err = p.rw.Read(d[pos:]) {
	}
	if err != nil {
		return []byte{}, nil
	}
	//if d[l] != '\r' || d[l+1] != '\n' {
	//	return []byte{}, "不正确的数据格式"
	//}
	return d[:l], nil
}

func (p *Parser) read_one_request() error {
	flag, err := p.get_flag()
	if err != nil {
		return err
	}
	if flag != '*' {
		str, err := p.get_string()
		if err != nil {
			return err
		}
		if err = p.fail([]byte("ERR unknown command '"), []byte{flag}, []byte(str), []byte{'\''}); err != nil {
			return err
		}
		return nil
	}
	m, err := p.get_int()
	if err != nil {
		if err = p.fail([]byte("ERR Protocol error: invalid multibulk length")); err != nil {
			return err
		}
		return err
	}
	for i := 0; i < m; i++ {
	}
	return nil
}

func (p *Parser) read_one_response() error {
	return nil
}

func (p *Parser) read_routine() {
	defer close(p.cmd)
	for {
		var err error
		if p.server {
			err = p.read_one_request()
		} else {
			err = p.read_one_response()
		}
		if err != nil {
			return
		}
	}
}
