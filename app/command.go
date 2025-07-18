package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
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

func handleCommand(cmd Command, r *RESPreader, conn net.Conn) {
	switch strings.ToLower(cmd.cmd) {
	case "ping":
		r.Write(RespData{Type: SimpleString, Str: "PONG"})
	case "echo":
		r.Write(RespData{Type: BulkString, Str: cmd.args[0]})
	case "set":
		if len(cmd.args) == 2 {
			db.Add(cmd.args[0], cmd.args[1])
			r.Write(RespData{Type: SimpleString, Str: "OK"})
		} else if len(cmd.args) == 4 && strings.ToLower(cmd.args[2]) == "px" {
			num, err := strconv.Atoi(cmd.args[3])
			if err != nil {
				r.Write(RespData{Type: Error, Str: "wrong numeric value for expiry"})
			}
			db.Addex(cmd.args[0], cmd.args[1], int64(num))
			r.Write(RespData{Type: SimpleString, Str: "OK"})
		} else {
			r.Write(RespData{Type: Error, Str: "ERR wrong number of arguments for 'set' command"})
		}
		if db.replicationInfo.IsMaster {
			db.cmdQueue <- cmd
		}
	case "get":
		if len(cmd.args) == 1 {
			val := db.Get(cmd.args[0])
			if val == nil {
				r.Write(RespData{Type: BulkString, IsNull: true})
			} else {
				r.Write(RespData{Type: BulkString, Str: *val})
			}
		} else {
			r.Write(RespData{Type: Error, Str: "ERR wrong number of arguments for 'get' command"})
		}
	case "save":
		if err := db.SaveRDB(); err != nil {
			r.Write(RespData{Type: Error, Str: fmt.Sprintf("ERR %v", err)})
		} else {
			r.Write(RespData{Type: SimpleString, Str: "OK"})
		}
	case "config":
		switch strings.ToLower(cmd.args[0]) {
		case "get":
			switch cmd.args[1] {
			case "dir":
				nameOfParam := RespData{Type: BulkString, Str: "dir", IsNull: false}
				valueofParam := RespData{Type: BulkString, Str: db.dir, IsNull: false}
				array := []RespData{nameOfParam, valueofParam}
				r.WriteArray(array)
			case "dbfilename":
				nameOfParam := RespData{Type: BulkString, Str: "dbfilename", IsNull: false}
				valueofParam := RespData{Type: BulkString, Str: db.dbfilename, IsNull: false}
				array := []RespData{nameOfParam, valueofParam}
				r.WriteArray(array)
			case "port":
				nameOfParam := RespData{Type: BulkString, Str: "port", IsNull: false}
				valueofParam := RespData{Type: BulkString, Str: db.port, IsNull: false}
				array := []RespData{nameOfParam, valueofParam}
				r.WriteArray(array)
			}
		case "set":
			switch cmd.args[1] {
			case "dir":
				db.dir = cmd.args[2]
				r.Write(RespData{Type: SimpleString, Str: "OK"})
			case "dbfilename":
				db.dbfilename = cmd.args[2]
				r.Write(RespData{Type: SimpleString, Str: "OK"})
			}
		}
	case "keys":
		if len(cmd.args) == 1 {
			keys := make([]RespData, 0, len(db.M))
			for k := range db.M {
				fmt.Println(k)
				keys = append(keys, RespData{Type: BulkString, Str: k, IsNull: false})
			}
			if keys == nil {
				r.Write(RespData{Type: Array, IsNull: true})
			} else {
				r.WriteArray(keys)
			}
		} else {
			r.Write(RespData{Type: Error, Str: "ERR wrong number of arguments for 'keys' command"})
		}
	case "info":
		WriteReplInfo(db.replicationInfo, &conn, r)
	case "replconf":
		r.Write(RespData{Type: SimpleString, Str: "OK"})
	case "psync":
		r.Write(RespData{Type: SimpleString, Str: fmt.Sprintf("FULLRESYNC %s %d",
			db.replicationInfo.masterReplID, db.replicationInfo.masterReplOffset)})
		sendEmptyRDB(conn)
		addReplica(db, &conn)
	default:
		r.Write(RespData{Type: Error, Str: "ERR unknown command '" + cmd.cmd + "'"})
	}
}

func handleSetCommand(db *DataBase, cmd Command) {
	log.Println(cmd.cmd)
	for _, arg := range cmd.args {
		log.Println(arg)
	}
	if len(cmd.args) == 2 {
		db.Add(cmd.args[0], cmd.args[1])
	} else if len(cmd.args) == 4 && strings.ToLower(cmd.args[2]) == "px" {
		num, err := strconv.Atoi(cmd.args[3])
		if err != nil {
			return
		}
		db.Addex(cmd.args[0], cmd.args[1], int64(num))
	}
}
