package protocol

import (
	"bufio"
	"io"
)

type (
	parse_state interface {
		process(*bufio.Reader) parse_state
	}

	Request struct {
	}

	Response struct {
	}

	Parser struct {
		r   *bufio.Reader
		cmd chan Request
	}

	init_state struct {
	}
)

func NewParser(r io.Reader) *Parser {
	ret := &Parser{}
	ret.r = bufio.NewReaderSize(r, 4096)
	ret.cmd = make(chan Request, 1)
	return nil
}

func (p *Parser) GetMonitor() <-chan Request {
	return p.cmd
}

func (p *Parser) ParseString(cmd string) []byte {
	return []byte{}
}

func (i *init_state) process(r *bufio.Reader) parse_state {
	return i
}

func (p *Parser) read_routine() {
	//data := make([]byte, 4096)
	for {
		flag, err := p.r.ReadByte()
		if err != nil {
			return
		}
		switch flag {
		case '*':
		case '$':
		case ':':
		case '+':
		case '-':
		default:
			close(p.cmd)
			return
		}
	}
}
