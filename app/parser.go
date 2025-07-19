package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// RespType represents the type of RESP data
type RespType int

const (
	SimpleString RespType = iota // +
	Error                        // -
	Integer                      // :
	BulkString                   // $
	Array                        // *
)

// RespData represents a RESP data structure
type RespData struct {
	Type   RespType
	Str    string     // for SimpleString, Error, and BulkString
	Num    int64      // for Integer
	Array  []RespData // for Array
	IsNull bool       // for null bulk strings ($-1) or null arrays (*-1)
}

// String returns a string representation of the RESP data
func (r RespData) String() string {
	switch r.Type {
	case SimpleString:
		return fmt.Sprintf("%s", r.Str)
	case Error:
		return fmt.Sprintf("%s", r.Str)
	case Integer:
		return fmt.Sprintf("%d", r.Num)
	case BulkString:
		if r.IsNull {
			return "BulkString(null)"
		}
		return fmt.Sprintf("%s", r.Str)
	case Array:
		if r.IsNull {
			return "Array(null)"
		}
		return fmt.Sprintf("%v", r.Array)
	default:
		return "Unknown"
	}
}

// IsError returns true if the RESP data represents an error
func (r RespData) IsError() bool {
	return r.Type == Error
}

type RESPreader struct {
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewRESPreader(conn net.Conn) *RESPreader {
	return &RESPreader{
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (r *RESPreader) Read() (RespData, int, error) {
	bytesRead := 0
	firstByte, err := r.reader.ReadByte()
	if err != nil {
		return RespData{}, 0, err
	}
	bytesRead++
	switch firstByte {
	case '+':
		str, n, err := r.ReadString()
		bytesRead += n
		return RespData{Type: SimpleString, Str: str}, bytesRead, err

	case '-':
		str, n, err := r.ReadString()
		bytesRead += n
		return RespData{Type: Error, Str: str}, bytesRead, err

	case ':':
		num, n, err := r.ReadInt()
		bytesRead += n
		return RespData{Type: Integer, Num: int64(num)}, bytesRead, err

	case '$':
		str, n, isNull, err := r.ReadBulk()
		bytesRead += n
		return RespData{Type: BulkString, Str: str, IsNull: isNull}, bytesRead, err

	case '*':
		arr, n, isNull, err := r.ReadArray()
		bytesRead += n
		return RespData{Type: Array, Array: arr, IsNull: isNull}, bytesRead, err

	default:
		return RespData{}, 0, fmt.Errorf("invalid RESP type byte: %c", firstByte)
	}

}

func (r *RESPreader) ReadArray() ([]RespData, int, bool, error) {
	// log.Println("Reading array")
	bytesRead := 0
	count, n, err := r.ReadInt()
	if err != nil {
		return nil, 0, false, err
	}
	if count == -1 {
		return nil, 0, true, nil // Null array
	}
	if count < 0 {
		return nil, 0, false, fmt.Errorf("invalid array length: %d", count)
	}
	bytesRead += n

	result := make([]RespData, 0, count)
	for range count {
		item, n, err := r.Read()
		bytesRead += n
		if err != nil {
			return nil, 0, false, err
		}
		result = append(result, item)
	}
	return result, bytesRead, false, nil

}

func (r *RESPreader) ReadBulk() (string, int, bool, error) {
	// log.Println("Reading bulk")
	bytesRead := 0
	length, n, err := r.ReadInt()
	bytesRead += n
	if err != nil {
		return "", 0, false, err
	}
	if length == -1 {
		return "", 0, true, nil // Null bulk string
	}
	if length < 0 {
		return "", 0, false, fmt.Errorf("invalid bulk string length: %d", length)
	}
	buf := make([]byte, length)
	buflen, err := io.ReadFull(r.reader, buf)
	bytesRead += buflen
	if err != nil {
		return "", 0, false, err
	}
	// Expect \r\n after the string
	cr, err := r.reader.ReadByte()
	bytesRead++
	if err != nil {
		return "", 0, false, err
	}
	lf, err := r.reader.ReadByte()
	bytesRead++
	if err != nil {
		return "", 0, false, err
	}
	if cr != '\r' || lf != '\n' {
		return "", 0, false, errors.New("invalid bulk string termination")
	}
	// log.Printf("ReadBulk Completed: %s", string(buf))
	return string(buf), bytesRead, false, nil
}

func (r *RESPreader) ReadInt() (int, int, error) {
	val := 0
	bytesRead := 0
	var isNegative bool
	firstByte, err := r.reader.ReadByte()
	bytesRead++
	if err != nil {
		return 0, 0, err
	}
	switch firstByte {
	case '+':
		isNegative = false
	case '-':
		isNegative = true
	default:
		if firstByte < '0' || firstByte > '9' {
			return 0, 0, errors.New("invalid integer format: non-numeric byte")
		}
		isNegative = false
		val = int(firstByte - '0')
	}
	for {
		curr, err := r.reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		bytesRead++
		if curr == '\r' {
			next, err := r.reader.ReadByte()
			if err != nil {
				return 0, 0, err
			}
			if next != '\n' {
				return 0, 0, errors.New("invalid integer format: missing LF after CR")
			}
			break
		}
		if curr < '0' || curr > '9' {
			return 0, 0, errors.New("invalid integer format: non-numeric byte")
		}
		val = val*10 + int(curr-'0')
	}
	if isNegative {
		val = -val
	}
	return val, bytesRead, nil
}

func (r *RESPreader) ReadError() (error, int, error) {
	bytesRead := 0
	errorMsg, n, err := r.ReadString()
	bytesRead += n
	if err != nil {
		return nil, 0, err
	}
	return errors.New(string(errorMsg)), bytesRead, nil
}

func (r *RESPreader) ReadString() (string, int, error) {
	bytesRead := 0
	line := make([]byte, 0, 1024)
	for {
		curr, err := r.reader.ReadByte()
		if err != nil {
			return "", 0, err
		}
		bytesRead++
		if curr == '\r' {
			next, er := r.reader.ReadByte()
			if er != nil {
				return "", 0, er
			}
			bytesRead++
			if next == '\n' {
				return string(line), bytesRead, nil
			} else {
				line = append(line, curr)
			}
		} else {
			line = append(line, curr)
		}
	}
}

func (w *RESPreader) Write(data RespData) error {
	switch data.Type {
	case SimpleString:
		return w.WriteSimpleString(data.Str)
	case Error:
		return w.WriteError(data.Str)
	case Integer:
		return w.WriteInteger(data.Num)
	case BulkString:
		if data.IsNull {
			return w.WriteNull()
		}
		return w.WriteBulkString(data.Str)
	case Array:
		if data.IsNull {
			return w.WriteNullArray()
		}
		return w.WriteArray(data.Array)
	default:
		return fmt.Errorf("unknown RESP type: %v", data.Type)
	}
}

func (w *RESPreader) WriteSimpleString(s string) error {
	_, err := w.writer.WriteString("+" + s + "\r\n")
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *RESPreader) WriteError(s string) error {
	_, err := w.writer.WriteString("-" + s + "\r\n")
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *RESPreader) WriteInteger(i int64) error {
	_, err := w.writer.WriteString(":" + strconv.FormatInt(i, 10) + "\r\n")
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *RESPreader) WriteBulkString(s string) error {
	_, err := w.writer.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *RESPreader) WriteNull() error {
	_, err := w.writer.WriteString("$-1\r\n")
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *RESPreader) WriteNullArray() error {
	_, err := w.writer.WriteString("*-1\r\n")
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *RESPreader) WriteArray(arr []RespData) error {
	_, err := w.writer.WriteString("*" + strconv.Itoa(len(arr)) + "\r\n")
	if err != nil {
		return err
	}

	for _, item := range arr {
		err := w.writeWithoutFlush(item)
		if err != nil {
			return err
		}
	}

	return w.writer.Flush()
}

func (w *RESPreader) WriteCommand(cmd string, args ...string) error {
	// Create array of bulk strings for command and arguments
	items := make([]RespData, 1+len(args))
	items[0] = RespData{Type: BulkString, Str: cmd}
	for i, arg := range args {
		items[i+1] = RespData{Type: BulkString, Str: arg}
	}

	return w.WriteArray(items)
}

func (w *RESPreader) writeWithoutFlush(data RespData) error {
	switch data.Type {
	case SimpleString:
		_, err := w.writer.WriteString("+" + data.Str + "\r\n")
		return err
	case Error:
		_, err := w.writer.WriteString("-" + data.Str + "\r\n")
		return err
	case Integer:
		_, err := w.writer.WriteString(":" + strconv.FormatInt(data.Num, 10) + "\r\n")
		return err
	case BulkString:
		if data.IsNull {
			_, err := w.writer.WriteString("$-1\r\n")
			return err
		}
		_, err := w.writer.WriteString("$" + strconv.Itoa(len(data.Str)) + "\r\n" + data.Str + "\r\n")
		return err
	case Array:
		if data.IsNull {
			_, err := w.writer.WriteString("*-1\r\n")
			return err
		}
		// Write array length
		_, err := w.writer.WriteString("*" + strconv.Itoa(len(data.Array)) + "\r\n")
		if err != nil {
			return err
		}
		// Write each array element
		for _, item := range data.Array {
			err := w.writeWithoutFlush(item)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown RESP type: %v", data.Type)
	}
}
