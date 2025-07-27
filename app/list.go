package main

import "strconv"

func handleLPushCommand(cmd Command) RespData {
	if len(cmd.args) < 2 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'lpush' command"}
	}

	key := cmd.args[0]
	values := cmd.args[1:]

	count := db.LPush(key, values...)
	if count == -1 {
		return RespData{Type: Error, Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	return RespData{Type: Integer, Num: int64(count)}
}

func handleRPushCommand(cmd Command) RespData {
	if len(cmd.args) < 2 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := cmd.args[0]
	values := cmd.args[1:]

	count := db.RPush(key, values...)
	if count == -1 {
		return RespData{Type: Error, Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	}

	return RespData{Type: Integer, Num: int64(count)}
}

func handleLPopCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'lpop' command"}
	}

	value := db.LPop(cmd.args[0])
	if value == nil {
		return RespData{Type: BulkString, IsNull: true}
	}

	return RespData{Type: BulkString, Str: *value}
}

func handleRPopCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'rpop' command"}
	}

	value := db.RPop(cmd.args[0])
	if value == nil {
		return RespData{Type: BulkString, IsNull: true}
	}

	return RespData{Type: BulkString, Str: *value}
}

func handleLLenCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'llen' command"}
	}

	length := db.LLen(cmd.args[0])
	return RespData{Type: Integer, Num: int64(length)}
}

func handleLRangeCommand(cmd Command) RespData {
	if len(cmd.args) != 3 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'lrange' command"}
	}

	start, err1 := strconv.Atoi(cmd.args[1])
	stop, err2 := strconv.Atoi(cmd.args[2])

	if err1 != nil || err2 != nil {
		return RespData{Type: Error, Str: "ERR value is not an integer or out of range"}
	}

	values := db.LRange(cmd.args[0], start, stop)

	respArray := make([]RespData, len(values))
	for i, val := range values {
		respArray[i] = RespData{Type: BulkString, Str: val}
	}

	return RespData{Type: Array, Array: respArray}
}
