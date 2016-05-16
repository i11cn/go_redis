package protocol

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
)

type (
	RESTPart struct {
		Flag   byte
		Data   []byte
		Length int
	}
	REST struct {
		Success bool
		Message string
		Parts   []RESTPart
	}

	Parser struct {
		r *bufio.Reader
	}
)

func EncodeREST(r *REST) []byte {
	var buf bytes.Buffer
	total_part := len(r.Parts)
	if total_part > 0 {
		set_multi_parts(&buf, total_part)
		for _, p := range r.Parts {
			add_part(&buf, p)
		}
	} else if r.Success {
		buf.WriteByte('+')
		buf.WriteString(r.Message)
		buf.WriteString("\r\n")
	} else {
		buf.WriteByte('-')
		buf.WriteString(r.Message)
		buf.WriteString("\r\n")
	}
	return buf.Bytes()
}

func DumpREST(r *REST) string {
	if r == nil {
		return "<nil>"
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	buf.WriteString("Success:")
	if r.Success {
		buf.WriteString("true")
	} else {
		buf.WriteString("false")
	}
	buf.WriteString(" Message:\"")
	buf.WriteString(r.Message)
	buf.WriteString("\"\r\n")
	for _, p := range r.Parts {
		buf.WriteString("  {")
		buf.WriteString("Flag:'")
		buf.WriteString(string(p.Flag))
		buf.WriteString("' Data:\"")
		buf.WriteString(string(p.Data))
		buf.WriteString("\" Length:")
		buf.WriteString(strconv.Itoa(p.Length))
		buf.WriteString("}\r\n")
	}
	buf.WriteByte('}')
	return buf.String()
}

func NewParser(r io.Reader) *Parser {
	ret := &Parser{}
	ret.r = bufio.NewReaderSize(r, 65536)
	return ret
}

func (p *Parser) ReadREST() (*REST, error) {
	ret := &REST{Success: true, Parts: make([]RESTPart, 0)}
	part, err := p.read_rest_part()
	if err != nil {
		return nil, err
	}
	switch part.Flag {
	case '-':
		ret.Success = false
		fallthrough
	case '+':
		ret.Message = string(part.Data)
	case '*':
		for i := 0; i < part.Length; i++ {
			if pt, err := p.read_rest_part(); err != nil {
				return nil, err
			} else {
				ret.Parts = append(ret.Parts, *pt)
			}
		}
	default:
		ret.Parts = append(ret.Parts, *part)
	}
	return ret, nil
}

func (p *Parser) read_rest_part() (*RESTPart, error) {
	ret := &RESTPart{}
	line, _, err := p.r.ReadLine()
	if err != nil {
		return nil, err
	}
	read_block := func(length int) ([]byte, error) {
		d := make([]byte, length+2)
		pos := 0
		var err error
		for n := 0; n < (length+2-pos) && err == nil; n, err = p.r.Read(d[pos:]) {
			pos += n
		}
		if err != nil {
			return nil, err
		}
		// TODO 可以在此处检查结尾是否\r\n，Redis的默认实现是不检查，直接跳过2个字节
		return d[:length], err
	}
	ret.Flag = line[0]
	switch ret.Flag {
	case '+', '-':
		ret.Data = line[1:]
	case ':', '*':
		if l, err := strconv.ParseInt(string(line[1:]), 10, 32); err != nil {
			return nil, err
		} else {
			ret.Length = int(l)
		}
	case '$':
		if l, err := strconv.ParseInt(string(line[1:]), 10, 32); err != nil {
			return nil, err
		} else if ret.Data, err = read_block(int(l)); err != nil {
			return nil, err
		}
	default:
		ret.Flag = 0
		ret.Data = line
	}
	return ret, nil
}

func set_multi_parts(buf *bytes.Buffer, i int) {
	buf.WriteByte('*')
	buf.WriteString(strconv.Itoa(i))
	buf.WriteString("\r\n")

}

func add_part(buf *bytes.Buffer, p RESTPart) {
	if len(p.Data) > 0 {
		buf.WriteByte('$')
		buf.WriteString(strconv.Itoa(len(p.Data)))
		buf.WriteString("\r\n")
		buf.Write(p.Data)
	} else {
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(p.Length))
	}
	buf.WriteString("\r\n")
}
