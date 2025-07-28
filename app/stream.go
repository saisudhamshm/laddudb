package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type StreamWaiter struct {
	Keys     []string
	IDs      []string
	Count    int
	Response chan map[string][]StreamEntry
}

type Stream struct {
	Entries []StreamEntry
	LastID  string
	Waiters []*StreamWaiter // For blocking reads
}
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

func generateStreamID(userID string) string {
	now := time.Now().UnixMilli()

	if userID == "" || userID == "*" {
		// Auto-generate ID: timestamp-0
		return fmt.Sprintf("%d-0", now)
	}

	// If user provides partial ID like "1234567890-*"
	if strings.HasSuffix(userID, "-*") {
		timestamp := strings.TrimSuffix(userID, "-*")
		return timestamp + "-0"
	}

	// Return user-provided ID as-is
	return userID
}

func compareStreamIDs(id1, id2 string) int {
	parts1 := strings.Split(id1, "-")
	parts2 := strings.Split(id2, "-")

	// Compare timestamps
	ts1, _ := strconv.ParseInt(parts1[0], 10, 64)
	ts2, _ := strconv.ParseInt(parts2[0], 10, 64)

	if ts1 != ts2 {
		if ts1 < ts2 {
			return -1
		}
		return 1
	}

	// Compare sequence numbers
	seq1, _ := strconv.ParseInt(parts1[1], 10, 64)
	seq2, _ := strconv.ParseInt(parts2[1], 10, 64)

	if seq1 < seq2 {
		return -1
	} else if seq1 > seq2 {
		return 1
	}
	return 0
}

func handleXAddCommand(cmd Command) RespData {
	if len(cmd.args) < 3 || len(cmd.args)%2 != 0 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'xadd' command"}
	}

	key := cmd.args[0]
	id := cmd.args[1]

	// Parse field-value pairs
	fields := make(map[string]string)
	for i := 2; i < len(cmd.args); i += 2 {
		if i+1 >= len(cmd.args) {
			return RespData{Type: Error, Str: "ERR wrong number of arguments for 'xadd' command"}
		}
		fields[cmd.args[i]] = cmd.args[i+1]
	}

	generatedID, err := db.XAdd(key, id, fields)
	if err != nil {
		return RespData{Type: Error, Str: err.Error()}
	}

	return RespData{Type: BulkString, Str: generatedID}
}

func handleXLenCommand(cmd Command) RespData {
	if len(cmd.args) != 1 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'xlen' command"}
	}

	length := db.XLen(cmd.args[0])
	return RespData{Type: Integer, Num: length}
}

func handleXRangeCommand(cmd Command) RespData {
	if len(cmd.args) < 3 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'xrange' command"}
	}

	key := cmd.args[0]
	start := cmd.args[1]
	end := cmd.args[2]

	var count int = -1
	if len(cmd.args) >= 5 && strings.ToLower(cmd.args[3]) == "count" {
		var err error
		count, err = strconv.Atoi(cmd.args[4])
		if err != nil {
			return RespData{Type: Error, Str: "ERR value is not an integer or out of range"}
		}
	}

	entries := db.XRange(key, start, end, count)

	// Convert to RESP format
	respArray := make([]RespData, len(entries))
	for i, entry := range entries {
		// Each entry: [ID, [field1, value1, field2, value2, ...]]
		fieldArray := make([]RespData, 0, len(entry.Fields)*2)
		for field, value := range entry.Fields {
			fieldArray = append(fieldArray,
				RespData{Type: BulkString, Str: field},
				RespData{Type: BulkString, Str: value},
			)
		}

		respArray[i] = RespData{
			Type: Array,
			Array: []RespData{
				{Type: BulkString, Str: entry.ID},
				{Type: Array, Array: fieldArray},
			},
		}
	}

	return RespData{Type: Array, Array: respArray}
}

func handleXReadCommand(cmd Command) RespData {
	if len(cmd.args) < 3 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'xread' command"}
	}

	var count int = -1
	var blockMs int64 = -1
	argIndex := 0

	// Handle COUNT option
	if argIndex < len(cmd.args) && strings.ToLower(cmd.args[argIndex]) == "count" {
		if argIndex+1 >= len(cmd.args) {
			return RespData{Type: Error, Str: "ERR syntax error"}
		}
		var err error
		count, err = strconv.Atoi(cmd.args[argIndex+1])
		if err != nil {
			return RespData{Type: Error, Str: "ERR value is not an integer or out of range"}
		}
		argIndex += 2
	}

	// Handle BLOCK option
	if argIndex < len(cmd.args) && strings.ToLower(cmd.args[argIndex]) == "block" {
		if argIndex+1 >= len(cmd.args) {
			return RespData{Type: Error, Str: "ERR syntax error"}
		}
		var err error
		blockMs, err = strconv.ParseInt(cmd.args[argIndex+1], 10, 64)
		if err != nil {
			return RespData{Type: Error, Str: "ERR value is not an integer or out of range"}
		}
		argIndex += 2
	}

	// Must have STREAMS keyword
	if argIndex >= len(cmd.args) || strings.ToLower(cmd.args[argIndex]) != "streams" {
		return RespData{Type: Error, Str: "ERR syntax error"}
	}
	argIndex++

	// Parse keys and IDs
	remainingArgs := cmd.args[argIndex:]
	if len(remainingArgs)%2 != 0 {
		return RespData{Type: Error, Str: "ERR Unbalanced XREAD list of streams"}
	}

	streamCount := len(remainingArgs) / 2
	keys := remainingArgs[:streamCount]
	ids := remainingArgs[streamCount:]

	var result map[string][]StreamEntry
	var err error

	if blockMs >= 0 {
		result, err = db.XReadBlocking(keys, ids, count, blockMs)
	} else {
		result = db.XRead(keys, ids, count)
	}

	if err != nil {
		return RespData{Type: Error, Str: err.Error()}
	}

	// Convert to RESP format
	if len(result) == 0 {
		return RespData{Type: BulkString, IsNull: true}
	}

	respArray := make([]RespData, 0, len(result))
	for key, entries := range result {
		entryArray := make([]RespData, len(entries))
		for i, entry := range entries {
			fieldArray := make([]RespData, 0, len(entry.Fields)*2)
			for field, value := range entry.Fields {
				fieldArray = append(fieldArray,
					RespData{Type: BulkString, Str: field},
					RespData{Type: BulkString, Str: value},
				)
			}

			entryArray[i] = RespData{
				Type: Array,
				Array: []RespData{
					{Type: BulkString, Str: entry.ID},
					{Type: Array, Array: fieldArray},
				},
			}
		}

		respArray = append(respArray, RespData{
			Type: Array,
			Array: []RespData{
				{Type: BulkString, Str: key},
				{Type: Array, Array: entryArray},
			},
		})
	}

	return RespData{Type: Array, Array: respArray}
}

func handleXAddCommandSlave(db *DataBase, cmd Command) error {
	if len(cmd.args) < 3 || len(cmd.args)%2 == 0 {
		return fmt.Errorf("ERR wrong number of arguments for 'xadd' command")
	}

	key := cmd.args[0]
	id := cmd.args[1]

	// Parse field-value pairs
	fields := make(map[string]string)
	for i := 2; i < len(cmd.args); i += 2 {
		if i+1 >= len(cmd.args) {
			return fmt.Errorf("ERR wrong number of arguments for 'xadd' command")
		}
		fields[cmd.args[i]] = cmd.args[i+1]
	}

	_, err := db.XAdd(key, id, fields)
	if err != nil {
		return err
	}

	return nil

}
