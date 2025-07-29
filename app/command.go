package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Command struct {
	cmd  string
	args []string
}
type ClientConn struct {
	conn             net.Conn
	transactionQueue []Command
	isTransaction    bool
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

// executeCommand handles the command logic and returns RespData
func executeCommand(cmd Command, clientConn *ClientConn, context bool) RespData {
	switch strings.ToLower(cmd.cmd) {
	case "multi":
		return handleMultiCommand(cmd, clientConn)
	case "exec":
		return handleExecCommand(clientConn)
	case "discard":
		return handleDiscardCommand(clientConn)

	}
	if clientConn.isTransaction && !context {
		clientConn.transactionQueue = append(clientConn.transactionQueue, cmd)
		return RespData{Type: SimpleString, Str: "QUEUED"}
	}

	switch strings.ToLower(cmd.cmd) {
	case "ping":
		return RespData{Type: SimpleString, Str: "PONG"}

	case "echo":
		return RespData{Type: BulkString, Str: cmd.args[0]}

	case "set":
		return handleSetCommand(cmd)
	case "delete":
		return handleDeleteCommand(cmd)

	case "get":
		return handleGetCommand(cmd)

	case "save":
		if err := db.SaveRDB(); err != nil {
			return RespData{Type: Error, Str: fmt.Sprintf("ERR %v", err)}
		}
		return RespData{Type: SimpleString, Str: "OK"}

	case "config":
		return handleConfigCommand(cmd)

	case "keys":
		return handleKeysCommand(cmd)

	case "info":
		return handleInfoCommand()

	case "replconf":
		return handleReplConfCommand(cmd)

	case "psync":
		return handlePsyncCommand(clientConn)

	case "wait":
		return handleWaitCommand()

	case "incr":
		return handleIncrCommand(cmd)

	case "lpush":
		return handleLPushCommand(cmd)
	case "rpush":
		return handleRPushCommand(cmd)
	case "lpop":
		return handleLPopCommand(cmd)
	case "rpop":
		return handleRPopCommand(cmd)
	case "llen":
		return handleLLenCommand(cmd)
	case "lrange":
		return handleLRangeCommand(cmd)
	case "type":
		return handleTypeCommand(cmd)
	case "xadd":
		return handleXAddCommand(cmd)
	case "xlen":
		return handleXLenCommand(cmd)
	case "xrange":
		return handleXRangeCommand(cmd)
	case "xread":
		return handleXReadCommand(cmd)

	default:
		return RespData{Type: Error, Str: "ERR unknown command '" + cmd.cmd + "'"}
	}
}

// handleCommand executes the command and writes the result
func handleCommand(cmd Command, r *RESPreader, clientConn *ClientConn) {
	result := executeCommand(cmd, clientConn, false)

	// Handle special cases that need additional operations after getting result
	switch strings.ToLower(cmd.cmd) {
	case "psync":
		r.Write(result)
		sendEmptyRDB(clientConn.conn)
		addReplica(db, &clientConn.conn)
		return

	case "wait":
		handleWaitResponse(cmd, r)
		return

	case "info":
		WriteReplInfo(db.replicationInfo, &clientConn.conn, r)
		return

	case "config":
		if strings.ToLower(cmd.args[0]) == "get" {
			handleConfigGetResponse(cmd, r)
			return
		}
	}

	// Standard response writing
	if result.Type == Array && len(result.Array) > 0 {
		r.WriteArray(result.Array)
	} else {
		r.Write(result)
	}

	// Handle replication for master
	if db.replicationInfo.IsMaster && shouldReplicate(cmd.cmd) {
		db.cmdQueue <- cmd
	}
}

func handleTypeCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'type' command"}
	}

	val := db.GetType(cmd.args[0])
	if val == nil {
		return RespData{Type: SimpleString, Str: "none"}
	}

	return RespData{Type: SimpleString, Str: *val}
}

// Helper functions for individual command logic
func handleSetCommand(cmd Command) RespData {
	if len(cmd.args) == 2 {
		db.Add(cmd.args[0], cmd.args[1])
		return RespData{Type: SimpleString, Str: "OK"}
	} else if len(cmd.args) == 4 && strings.ToLower(cmd.args[2]) == "px" {
		num, err := strconv.Atoi(cmd.args[3])
		if err != nil {
			return RespData{Type: Error, Str: "wrong numeric value for expiry"}
		}
		db.Addex(cmd.args[0], cmd.args[1], int64(num))
		atomic.StoreInt64(&db.ackCount, 0)
		return RespData{Type: SimpleString, Str: "OK"}
	}
	return RespData{Type: Error, Str: "ERR wrong number of arguments for 'set' command"}
}

func handleGetCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'get' command"}
	}

	val := db.Get(cmd.args[0])
	if val == nil {
		return RespData{Type: BulkString, IsNull: true}
	}
	return RespData{Type: BulkString, Str: *val}
}

func handleConfigCommand(cmd Command) RespData {
	if len(cmd.args) < 2 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for config command"}
	}

	switch strings.ToLower(cmd.args[0]) {
	case "get":
		return handleConfigGet(cmd.args[1])
	case "set":
		return handleConfigSet(cmd.args[1], cmd.args[2])
	default:
		return RespData{Type: Error, Str: "ERR unknown config subcommand"}
	}
}

func handleConfigGet(param string) RespData {
	switch param {
	case "dir":
		return RespData{
			Type: Array,
			Array: []RespData{
				{Type: BulkString, Str: "dir"},
				{Type: BulkString, Str: db.dir},
			},
		}
	case "dbfilename":
		return RespData{
			Type: Array,
			Array: []RespData{
				{Type: BulkString, Str: "dbfilename"},
				{Type: BulkString, Str: db.dbfilename},
			},
		}
	case "port":
		return RespData{
			Type: Array,
			Array: []RespData{
				{Type: BulkString, Str: "port"},
				{Type: BulkString, Str: db.port},
			},
		}
	default:
		return RespData{Type: Array, IsNull: true}
	}
}

func handleConfigSet(param, value string) RespData {
	switch param {
	case "dir":
		db.dir = value
		return RespData{Type: SimpleString, Str: "OK"}
	case "dbfilename":
		db.dbfilename = value
		return RespData{Type: SimpleString, Str: "OK"}
	default:
		return RespData{Type: Error, Str: "ERR unsupported config parameter"}
	}
}

func handleKeysCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'keys' command"}
	}

	keys := make([]RespData, 0, len(db.M))
	for k := range db.M {
		fmt.Println(k)
		keys = append(keys, RespData{Type: BulkString, Str: k})
	}

	if len(keys) == 0 {
		return RespData{Type: Array, IsNull: true}
	}
	return RespData{Type: Array, Array: keys}
}

func handleInfoCommand() RespData {
	// This is a special case that needs custom handling in handleCommand
	return RespData{Type: SimpleString, Str: "INFO_RESPONSE"}
}

func handleReplConfCommand(cmd Command) RespData {
	if len(cmd.args) == 0 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for replconf command"}
	}

	switch strings.ToLower(cmd.args[0]) {
	case "getack":
		return RespData{
			Type: Array,
			Array: []RespData{
				{Type: BulkString, Str: "REPLCONF"},
				{Type: BulkString, Str: "ACK"},
				{Type: BulkString, Str: fmt.Sprintf("%d", db.replicationInfo.ReplOffset)},
			},
		}
	case "ack":
		if len(cmd.args) < 2 {
			return RespData{Type: Error, Str: "ERR wrong number of arguments for replconf ack"}
		}
		_, err := strconv.ParseInt(cmd.args[1], 10, 64)
		if err != nil {
			log.Printf("Error parsing REPLCONF ACK offset '%s': %v", cmd.args[1], err)
			return RespData{Type: Error, Str: "ERR invalid offset"}
		}
		atomic.AddInt64(&db.ackCount, 1)
		return RespData{Type: SimpleString, Str: "OK"}
	default:
		return RespData{Type: SimpleString, Str: "OK"}
	}
}

func handlePsyncCommand(clientConn *ClientConn) RespData {
	return RespData{
		Type: SimpleString,
		Str: fmt.Sprintf("FULLRESYNC %s %d",
			db.replicationInfo.masterReplID,
			db.replicationInfo.masterReplOffset),
	}
}

func handleWaitCommand() RespData {
	// This needs special handling in handleCommand due to timing logic
	return RespData{Type: SimpleString, Str: "WAIT_RESPONSE"}
}

func handleIncrCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'incr' command"}
	}

	err := db.Incr(cmd.args[0])
	if err != nil {
		return RespData{Type: Error, Str: err.Error()}
	}
	return RespData{Type: SimpleString, Str: "OK"}
}

// Special response handlers for complex cases
func handleWaitResponse(cmd Command, r *RESPreader) {
	db.cmdQueue <- Command{cmd: "REPLCONF", args: []string{"GETACK", "*"}}

	requiredAcks, err := strconv.Atoi(cmd.args[0])
	if err != nil {
		r.Write(RespData{Type: Error, Str: "ERR invalid number format"})
		return
	}
	timeoutMS, err := strconv.Atoi(cmd.args[1])
	if err != nil {
		r.Write(RespData{Type: Error, Str: "ERR invalid timeout format"})
		return
	}

	timeout := time.Duration(timeoutMS) * time.Millisecond
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeoutChannel := time.After(timeout)
	for {
		select {
		case <-ticker.C:
			if db.ackCount >= int64(requiredAcks) {
				response := fmt.Sprintf(":%d\r\n", db.ackCount)
				r.Write(RespData{Type: SimpleString, Str: response})
				return
			}
		case <-timeoutChannel:
			response := fmt.Sprintf(":%d\r\n", db.ackCount)
			r.Write(RespData{Type: SimpleString, Str: response})
			return
		}
	}
}

func handleConfigGetResponse(cmd Command, r *RESPreader) {
	result := handleConfigGet(cmd.args[1])
	if result.Type == Array {
		r.WriteArray(result.Array)
	} else {
		r.Write(result)
	}
}

// Helper function to determine if command should be replicated
func shouldReplicate(cmdName string) bool {
	replicatedCommands := map[string]bool{
		"set":    true,
		"incr":   true,
		"lpush":  true,
		"rpush":  true,
		"lpop":   true,
		"rpop":   true,
		"xadd":   true,
		"delete": true,
	}
	return replicatedCommands[strings.ToLower(cmdName)]
}

func handleIncrCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) != 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'incr' command")
	}

	err := db.Incr(cmd.args[0])
	if err != nil {
		return fmt.Errorf("ERR failed to increment key '%s': %s", cmd.args[0], err.Error())
	}
	return nil
}

func handleSetCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) == 2 {
		db.Add(cmd.args[0], cmd.args[1])
		return nil
	} else if len(cmd.args) == 4 && strings.ToLower(cmd.args[2]) == "px" {
		num, err := strconv.Atoi(cmd.args[3])
		if err != nil {
			return fmt.Errorf("ERR wrong numeric value for expiry")
		}
		db.Addex(cmd.args[0], cmd.args[1], int64(num))
		return nil
	}
	return fmt.Errorf("ERR wrong number of arguments for 'set' command")
}

func handleLPUSHCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) != 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'lpush' command")
	}

	key := cmd.args[0]
	values := cmd.args[1:]

	count := db.LPush(key, values...)
	if count == -1 {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return nil
}

func handleRPUSHCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) != 2 {
		return fmt.Errorf("ERR wrong number of arguments for 'rpush' command")
	}

	key := cmd.args[0]
	values := cmd.args[1:]

	count := db.RPush(key, values...)
	if count == -1 {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return nil
}

func handleLPOPCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) != 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'lpop' command")
	}

	db.LPop(cmd.args[0])

	return nil
}

func handleRPOPCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) != 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'rpop' command")
	}
	db.RPop(cmd.args[0])
	return nil
}

func handleDeleteCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) != 1 {
		return fmt.Errorf("ERR wrong number of arguments for 'delete' command")
	}

	db.Delete(cmd.args[0])

	return nil
}

func handleDeleteCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'delete' command"}
	}

	db.Delete(cmd.args[0])

	return RespData{Type: SimpleString, Str: "OK"}
}
