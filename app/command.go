package main

import (
	"fmt"
)

type Command struct {
	cmd  string
	args []string
}

func parseCmd(r RespData) (Command, error) {
	if r.Type == SimpleString && r.Str == "PING" {
		return Command{cmd: "PING", args: nil}, nil
	}
	if r.Type != Array {
		return Command{}, fmt.Errorf("expected array, got %v", r.Type)
	}

	if len(r.Array) < 1 {
		return Command{}, fmt.Errorf("expected at least one element in array")
	}

	cmd := r.Array[0].String()
	args := make([]string, len(r.Array)-1)
	for i, item := range r.Array[1:] {
		if item.Type != BulkString {
			return Command{}, fmt.Errorf("expected bulk string, got %v", item.Type)
		}
		args[i] = item.Str
	}

	return Command{cmd: cmd, args: args}, nil
}
