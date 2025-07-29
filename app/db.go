package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/parser"
)

type DataBase struct {
	M                    map[string]DBentry
	dir                  string
	dbfilename           string
	port                 string
	rdbVersion           int
	replicationInfo      ReplicationInfo
	cmdQueue             chan Command
	acknowledgedReplicas map[*net.Conn]int
	ackCount             int64
	replicaMutex         sync.Mutex
	mu                   sync.RWMutex
	streamWaiters        map[string][]*StreamWaiter // key -> waiters
	waiterMutex          sync.RWMutex
}
type DataType int

const (
	StringType DataType = iota
	ListType
	StreamType
)

type DBentry struct {
	dataType  DataType
	val       string
	list      []string
	stream    *Stream
	expiresAt int64
	timestamp int64
}

func (entry *DBentry) IsString() bool {
	return entry.dataType == StringType
}

func (entry *DBentry) IsList() bool {
	return entry.dataType == ListType
}

func (db *DataBase) Addex(key string, val string, expiresAt int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.M[key] = DBentry{StringType, val, nil, nil, expiresAt, time.Now().UnixMilli()}
}

func (db *DataBase) Add(key string, val string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.M[key] = DBentry{StringType, val, nil, nil, -1, time.Now().UnixMilli()}
}

func (db *DataBase) Incr(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if entry, ok := db.M[key]; ok {
		curr_val, err := strconv.Atoi(entry.val)
		if err != nil {
			return err
		}
		entry.val = strconv.Itoa(curr_val + 1)
		db.M[key] = entry
	} else {
		db.M[key] = DBentry{StringType, "1", nil, nil, -1, time.Now().UnixMilli()}
	}
	return nil
}

func (db *DataBase) Get(key string) *string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if entry, ok := db.M[key]; ok {
		if !entry.IsString() {
			return nil
		}
		if entry.expiresAt == -1 || entry.expiresAt+entry.timestamp >= time.Now().UnixMilli() {
			return &entry.val
		} else {
			delete(db.M, key)
			return nil
		}
	}
	return nil
}

func (db *DataBase) GetType(key string) *string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if entry, ok := db.M[key]; ok {
		var typeStr string
		switch entry.dataType {
		case StringType:
			typeStr = "string"
		case ListType:
			typeStr = "list"
		default:
			typeStr = "unknown" // Fallback for any future types not explicitly handled
		}
		return &typeStr
	}
	return nil
}

func NewDatabase(dir, dbfilename, port, masterAddr string) *DataBase {
	var isMaster bool
	if masterAddr == "" {
		isMaster = true
	} else {
		isMaster = false
	}
	db := &DataBase{
		M:          make(map[string]DBentry),
		dir:        dir,
		dbfilename: dbfilename,
		port:       port,
		rdbVersion: 10,
		replicationInfo: ReplicationInfo{
			IsMaster:         isMaster,
			masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			masterReplOffset: 0,
			masterAddr:       masterAddr,
			replicas:         []*net.Conn{},
		},
		cmdQueue:             make(chan Command),
		acknowledgedReplicas: make(map[*net.Conn]int),
		ackCount:             0,
		replicaMutex:         sync.Mutex{},
		mu:                   sync.RWMutex{},
		streamWaiters:        make(map[string][]*StreamWaiter),
		waiterMutex:          sync.RWMutex{},
	}
	return db
}

func (db *DataBase) init() {
	if !db.replicationInfo.IsMaster {
		conn, err := handShake(db.replicationInfo.masterAddr, db.port)
		if err != nil {
			fmt.Println("Error connecting to master:", err)
			return
		}
		go db.listenToMaster(conn)
	} else {
		go db.propagateCommands()
	}
	if _, err := os.Stat(db.dir + "/" + db.dbfilename); err == nil {
		err := db.LoadRDB()
		if err != nil {
			fmt.Printf("Error loading RDB file: %v\n", err)
		}
	}
}

func (db *DataBase) propagateCommands() {
	for cmd := range db.cmdQueue {

		writeCmd := fmt.Sprintf("*%d\r\n", len(cmd.args)+1)
		writeCmd += fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.cmd), cmd.cmd)
		for _, arg := range cmd.args {
			writeCmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
		}
		if cmd.cmd == "SET" || cmd.cmd == "LPUSH" || cmd.cmd == "RPUSH" || cmd.cmd == "INCR" || cmd.cmd == "LPOP" || cmd.cmd == "RPOP" {
			atomic.AddInt64(&db.replicationInfo.masterReplOffset, int64(len(writeCmd)))
		}
		for _, conn := range db.replicationInfo.replicas {
			(*conn).Write([]byte(writeCmd))
			// log.Println("Command propagated to replica:", writeCmd)
		}
	}

}

func (db *DataBase) listenToMaster(conn *net.Conn) {
	(*conn).Read(make([]byte, 1024))
	r := NewRESPreader(*conn)
	log.Println("---Listening to master---")
	for {
		val, n, err := r.Read()
		// log.Println("Received command from master:", val)
		if err != nil {
			// log.Println("Error reading command from master:", err)
			continue
		}
		cmd, er := parseCmd(val)
		if er != nil {
			// log.Println("Error parsing command: ", er)
			continue
		}
		switch strings.ToLower(cmd.cmd) {
		case "replconf":
			log.Println("Received REPLCONF GETACK command")
			if strings.ToLower(cmd.args[0]) == "getack" {
				r.Write(RespData{Type: Array, Array: []RespData{
					{Type: BulkString, Str: "REPLCONF"},
					{Type: BulkString, Str: "ACK"},
					{Type: BulkString, Str: fmt.Sprintf("%d", db.replicationInfo.ReplOffset)}, // Replace with actual slave port
				}})
			}

		case "set":
			err := handleSetCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}
		case "incr":
			err := handleIncrCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}
			//	TODO : add error handling for set and incr
		case "ping":
			continue
		case "rpop":
			err := handleRPOPCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}
		case "rpush":
			err := handleRPUSHCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}
		case "lpush":
			err := handleLPUSHCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}
		case "lpop":
			err := handleLPUSHCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}
		case "xadd":
			err := handleXAddCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}
		case "delete":
			err := handleDeleteCommandSlave(db, cmd)
			if err == nil {
				db.replicationInfo.ReplOffset += n
			}

		default:
			log.Println("Received unknown command from master:", cmd.cmd)

		}

	}
}

func (db *DataBase) Delete(key string) {
	delete(db.M, key)
}

func (db *DataBase) SaveRDB() error {
	err := os.MkdirAll(db.dir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory %s: %v", db.dir, err)
	}

	rdbFile := db.dir + "/" + db.dbfilename
	f, err := os.Create(rdbFile)
	if err != nil {
		return fmt.Errorf("failed to create RDB file: %v", err)
	}
	defer f.Close()

	enc := encoder.NewEncoder(f)
	now := time.Now().UnixMilli()
	err = enc.WriteHeader()
	if err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	auxMap := map[string]string{
		"redis-ver":    "7.2.0",
		"redis-bits":   "64",
		"ctime":        fmt.Sprintf("%d", time.Now().Unix()),
		"aof-preamble": "0",
	}

	for key, value := range auxMap {
		err = enc.WriteAux(key, value)
		if err != nil {
			return fmt.Errorf("failed to write aux field %s: %w", key, err)
		}
	}

	// Only write DB header and entries if there are actual keys
	validKeys := 0
	for _, entry := range db.M {
		if entry.expiresAt == -1 || entry.expiresAt+entry.timestamp >= now {
			validKeys++
		}
	}

	if validKeys > 0 {
		err = enc.WriteDBHeader(0, uint64(validKeys), 0) // database 0, key count, TTL count
		if err != nil {
			return fmt.Errorf("failed to write database header: %w", err)
		}

		for key, entry := range db.M {

			if entry.expiresAt != -1 && entry.expiresAt+entry.timestamp < now {
				continue // Skip expired keys
			}
			switch entry.dataType {
			case StringType:
				if entry.expiresAt != -1 {
					expiry := entry.timestamp + entry.expiresAt
					err = enc.WriteStringObject(key, []byte(entry.val), encoder.WithTTL(uint64(expiry)))
					if err != nil {
						return fmt.Errorf("failed to write expiry: %v", err)
					}
				} else {
					err = enc.WriteStringObject(key, []byte(entry.val))
					if err != nil {
						return fmt.Errorf("failed to write key-value: %v", err)
					}
				}
			case ListType:
				listValues := make([][]byte, len(entry.list))
				for i, val := range entry.list {
					listValues[i] = []byte(val)
				}

				if entry.expiresAt != -1 {
					expiry := entry.timestamp + entry.expiresAt
					err = enc.WriteListObject(key, listValues, encoder.WithTTL(uint64(expiry)))
					if err != nil {
						return fmt.Errorf("failed to write expiry: %v", err)
					}
				} else {
					err = enc.WriteListObject(key, listValues)
					if err != nil {
						return fmt.Errorf("failed to write list: %v", err)
					}
				}

			case StreamType:
				streamData := make([][]byte, 0)
				for _, streamEntry := range entry.stream.Entries {
					// Format: "ID:field1=value1,field2=value2"
					entryData := streamEntry.ID + ":"
					fieldPairs := make([]string, 0, len(streamEntry.Fields))
					for field, value := range streamEntry.Fields {
						fieldPairs = append(fieldPairs, field+"="+value)
					}
					entryData += strings.Join(fieldPairs, ",")
					streamData = append(streamData, []byte(entryData))
				}
				if entry.expiresAt != -1 {
					expiry := entry.timestamp + entry.expiresAt
					err = enc.WriteListObject(key, streamData, encoder.WithTTL(uint64(expiry)))
				} else {
					err = enc.WriteListObject(key, streamData)
				}

			}

		}

	}

	err = enc.WriteEnd()
	if err != nil {
		return fmt.Errorf("failed to finalize RDB file: %v", err)
	}

	return nil
}

func sendEmptyRDB(conn net.Conn) error {
	// Create an in-memory buffer to hold the RDB data
	var rdbBuf bytes.Buffer

	// Create the RDB content in the buffer
	enc := encoder.NewEncoder(&rdbBuf)
	if err := enc.WriteHeader(); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}
	if err := enc.WriteEnd(); err != nil {
		return fmt.Errorf("failed to write end marker: %v", err)
	}

	// Get the RDB content
	rdbData := rdbBuf.Bytes()

	// Create the complete message with header and contents in a single buffer
	var message bytes.Buffer
	message.WriteString(fmt.Sprintf("$%d\r\n", len(rdbData)))
	message.Write(rdbData)

	// Send the complete message in one write
	_, err := conn.Write(message.Bytes())
	if err != nil {
		fmt.Printf("failed to send RDB data: %v", err)
		return fmt.Errorf("failed to send RDB data: %v", err)
	}
	return nil
}

func (db *DataBase) LoadRDB() error {
	// Open the RDB file
	rdbFilePath := db.dir + "/" + db.dbfilename
	rdbFile, err := os.Open(rdbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer rdbFile.Close()

	// Create decoder
	decoder := parser.NewDecoder(rdbFile)

	// Parse the RDB file
	err = decoder.Parse(func(o parser.RedisObject) bool {
		switch o.GetType() {
		case parser.StringType:
			str := o.(*parser.StringObject)

			// Check if key has expiration and if it's still valid
			if str.GetExpiration() != nil {
				expirationTime := *str.GetExpiration()
				if time.Now().After(expirationTime) {
					return true // Skip expired key
				}
				db.M[str.Key] = DBentry{
					dataType:  StringType,
					val:       string(str.Value),
					timestamp: time.Now().UnixMilli(),
					expiresAt: expirationTime.UnixMilli() - time.Now().UnixMilli(),
				}
			} else {
				db.M[str.Key] = DBentry{
					dataType:  StringType,
					val:       string(str.Value),
					timestamp: time.Now().UnixMilli(),
					expiresAt: -1,
				}
			}

		case parser.ListType:
			listObj := o.(*parser.ListObject)

			// Check expiration
			if listObj.GetExpiration() != nil {
				expirationTime := *listObj.GetExpiration()
				if time.Now().After(expirationTime) {
					return true // Skip expired key
				}
			}

			// Check if this is actually a stream stored as a list
			if db.isStreamData(listObj.Values) {
				// Parse as stream
				stream := db.parseStreamFromList(listObj.Values)

				var expiresAt int64 = -1
				if listObj.GetExpiration() != nil {
					expiresAt = listObj.GetExpiration().UnixMilli() - time.Now().UnixMilli()
				}

				db.M[listObj.Key] = DBentry{
					dataType:  StreamType,
					stream:    stream,
					timestamp: time.Now().UnixMilli(),
					expiresAt: expiresAt,
				}
			} else {
				// Parse as regular list
				listValues := make([]string, len(listObj.Values))
				for i, val := range listObj.Values {
					listValues[i] = string(val)
				}

				var expiresAt int64 = -1
				if listObj.GetExpiration() != nil {
					expiresAt = listObj.GetExpiration().UnixMilli() - time.Now().UnixMilli()
				}

				db.M[listObj.Key] = DBentry{
					dataType:  ListType,
					list:      listValues,
					timestamp: time.Now().UnixMilli(),
					expiresAt: expiresAt,
				}
			}
		}

		return true
	})

	if err != nil {
		return fmt.Errorf("failed to parse RDB file: %w", err)
	}

	return nil
}

// Helper function to detect if list data is actually stream data
func (db *DataBase) isStreamData(values [][]byte) bool {
	if len(values) == 0 {
		return false
	}

	// Check multiple entries to be more confident it's stream data
	entriesToCheck := len(values)
	if entriesToCheck > 3 {
		entriesToCheck = 3 // Check first 3 entries max
	}

	validStreamEntries := 0

	for i := 0; i < entriesToCheck; i++ {
		if db.isValidStreamEntry(string(values[i])) {
			validStreamEntries++
		}
	}

	// Consider it stream data if at least half of checked entries are valid stream format
	return validStreamEntries >= (entriesToCheck+1)/2
}

// Helper function to validate if a single entry matches stream format
func (db *DataBase) isValidStreamEntry(entryData string) bool {
	// Stream entries should have format: "timestamp-sequence:field1=value1,field2=value2,"
	colonIndex := strings.Index(entryData, ":")
	if colonIndex == -1 {
		return false
	}

	// Check if the part before colon looks like a stream ID (timestamp-sequence)
	idPart := entryData[:colonIndex]
	if !strings.Contains(idPart, "-") {
		return false
	}

	parts := strings.Split(idPart, "-")
	if len(parts) != 2 {
		return false
	}

	// Verify both parts are numeric (timestamp and sequence)
	timestamp, err1 := strconv.ParseInt(parts[0], 10, 64)
	sequence, err2 := strconv.ParseInt(parts[1], 10, 64)

	if err1 != nil || err2 != nil {
		return false
	}

	// Additional validation: timestamp should be reasonable
	now := time.Now().UnixMilli()
	if timestamp < 0 || timestamp > now+86400000 { // Allow 1 day in future
		return false
	}

	// Sequence should be non-negative
	if sequence < 0 {
		return false
	}

	// Validate the fields part (after colon)
	fieldsData := entryData[colonIndex+1:]
	return db.isValidFieldsFormat(fieldsData)
}

// Helper function to validate fields format
func (db *DataBase) isValidFieldsFormat(fieldsData string) bool {
	// Remove trailing comma if present
	fieldsData = strings.TrimSuffix(fieldsData, ",")

	if fieldsData == "" {
		return true // Empty fields after removing comma is valid
	}

	fieldPairs := strings.Split(fieldsData, ",")
	for _, pair := range fieldPairs {
		if pair == "" {
			continue // Skip empty pairs
		}

		// Each pair should have format "field=value"
		if !strings.Contains(pair, "=") {
			return false
		}

		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return false
		}

		// Field name should not be empty
		if strings.TrimSpace(parts[0]) == "" {
			return false
		}
	}

	return true
}

// Parse stream data from list format (using old StreamEntry structure)
func (db *DataBase) parseStreamFromList(values [][]byte) *Stream {
	stream := &Stream{
		Entries: make([]StreamEntry, 0, len(values)),
		LastID:  "",
		Waiters: make([]*StreamWaiter, 0),
	}

	for _, value := range values {
		entry := db.parseStreamEntry(string(value))
		if entry != nil {
			stream.Entries = append(stream.Entries, *entry)
			stream.LastID = entry.ID
		}
		// Skip invalid entries but continue processing
	}

	return stream
}

// Parse individual stream entry (using old StreamEntry structure)
func (db *DataBase) parseStreamEntry(entryData string) *StreamEntry {
	// Format: "ID:field1=value1,field2=value2,"
	colonIndex := strings.Index(entryData, ":")
	if colonIndex == -1 {
		return nil
	}

	id := entryData[:colonIndex]
	fieldsData := entryData[colonIndex+1:]

	// Validate the ID format
	if !db.isValidStreamID(id) {
		return nil
	}

	// Parse fields
	fields := make(map[string]string)

	if fieldsData != "" {
		// Remove trailing comma if present
		fieldsData = strings.TrimSuffix(fieldsData, ",")

		if fieldsData != "" {
			fieldPairs := strings.Split(fieldsData, ",")
			for _, pair := range fieldPairs {
				if pair == "" {
					continue
				}

				equalIndex := strings.Index(pair, "=")
				if equalIndex == -1 {
					continue // Skip malformed pairs
				}

				field := strings.TrimSpace(pair[:equalIndex])
				value := pair[equalIndex+1:]

				if field != "" {
					fields[field] = value
				}
			}
		}
	}

	// Return old StreamEntry structure
	return &StreamEntry{
		ID:     id,
		Fields: fields,
	}
}

// Helper function to validate stream ID format
func (db *DataBase) isValidStreamID(id string) bool {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return false
	}

	// Verify both parts are numeric
	timestamp, err1 := strconv.ParseInt(parts[0], 10, 64)
	sequence, err2 := strconv.ParseInt(parts[1], 10, 64)

	if err1 != nil || err2 != nil {
		return false
	}

	// Basic validation
	return timestamp >= 0 && sequence >= 0
}

func (db *DataBase) LPush(key string, values ...string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.M[key]
	if !exists {
		// Create new list
		db.M[key] = DBentry{
			dataType:  ListType,
			list:      append(values, []string{}...),
			timestamp: time.Now().UnixMilli(),
			expiresAt: -1,
		}
		return len(values)
	}

	if !entry.IsList() {
		return -1 // Wrong type error
	}

	// Prepend values to existing list
	newList := append(values, entry.list...)
	entry.list = newList
	db.M[key] = entry

	return len(entry.list)
}

func (db *DataBase) RPush(key string, values ...string) int {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.M[key]
	if !exists {
		// Create new list
		db.M[key] = DBentry{
			dataType:  ListType,
			list:      values,
			timestamp: time.Now().UnixMilli(),
			expiresAt: -1,
		}
		return len(values)
	}

	if !entry.IsList() {
		return -1 // Wrong type error
	}

	// Append values to existing list
	entry.list = append(entry.list, values...)
	db.M[key] = entry

	return len(entry.list)
}

func (db *DataBase) LPop(key string) *string {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.M[key]
	if !exists || !entry.IsList() || len(entry.list) == 0 {
		return nil
	}

	value := entry.list[0]
	entry.list = entry.list[1:]

	if len(entry.list) == 0 {
		delete(db.M, key)
	} else {
		db.M[key] = entry
	}

	return &value
}

func (db *DataBase) RPop(key string) *string {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry, exists := db.M[key]
	if !exists || !entry.IsList() || len(entry.list) == 0 {
		return nil
	}

	lastIndex := len(entry.list) - 1
	value := entry.list[lastIndex]
	entry.list = entry.list[:lastIndex]

	if len(entry.list) == 0 {
		delete(db.M, key)
	} else {
		db.M[key] = entry
	}

	return &value
}

func (db *DataBase) LLen(key string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	entry, exists := db.M[key]
	if !exists || !entry.IsList() {
		return 0
	}

	return len(entry.list)
}

func (db *DataBase) LRange(key string, start, stop int) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	entry, exists := db.M[key]
	if !exists || !entry.IsList() {
		return []string{}
	}

	listLen := len(entry.list)
	if listLen == 0 {
		return []string{}
	}

	// Handle negative indices
	if start < 0 {
		start = listLen + start
	}
	if stop < 0 {
		stop = listLen + stop
	}

	// Bounds checking
	if start < 0 {
		start = 0
	}
	if stop >= listLen {
		stop = listLen - 1
	}
	if start > stop {
		return []string{}
	}

	return entry.list[start : stop+1]
}

func (entry *DBentry) IsStream() bool {
	return entry.dataType == StreamType
}

func (entry *DBentry) GetStreamValue() *Stream {
	if entry.IsStream() {
		return entry.stream
	}
	return nil
}

func (db *DataBase) XAdd(key string, id string, fields map[string]string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Generate ID
	generatedID := generateStreamID(id)

	entry, exists := db.M[key]
	if !exists {
		// Create new stream
		stream := &Stream{
			Entries: []StreamEntry{},
			LastID:  "",
			Waiters: []*StreamWaiter{},
		}
		db.M[key] = DBentry{
			dataType:  StreamType,
			stream:    stream,
			timestamp: time.Now().UnixMilli(),
			expiresAt: -1,
		}
		entry = db.M[key]
	}

	if !entry.IsStream() {
		return "", fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := entry.stream

	// Validate ID is greater than last ID
	if stream.LastID != "" && compareStreamIDs(generatedID, stream.LastID) <= 0 {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	// Add entry
	streamEntry := StreamEntry{
		ID:     generatedID,
		Fields: fields,
	}

	stream.Entries = append(stream.Entries, streamEntry)
	stream.LastID = generatedID

	db.M[key] = entry

	// Notify waiting clients
	go db.notifyWaiters(key, streamEntry)

	return generatedID, nil
}

func (db *DataBase) notifyWaiters(key string, newEntry StreamEntry) {
	db.waiterMutex.Lock()
	defer db.waiterMutex.Unlock()

	waiters, exists := db.streamWaiters[key]
	if !exists {
		return
	}

	var remainingWaiters []*StreamWaiter

	for _, waiter := range waiters {
		// Check if this waiter is interested in this key
		shouldNotify := false
		for i, waiterKey := range waiter.Keys {
			if waiterKey == key && i < len(waiter.IDs) {
				// Check if new entry ID is greater than waiter's last seen ID
				if compareStreamIDs(newEntry.ID, waiter.IDs[i]) > 0 {
					shouldNotify = true
					break
				}
			}
		}

		if shouldNotify {
			// Send notification
			select {
			case waiter.Response <- map[string][]StreamEntry{key: {newEntry}}:
				// Successfully notified, don't keep this waiter
			default:
				// Channel full or closed, keep waiter for retry
				remainingWaiters = append(remainingWaiters, waiter)
			}
		} else {
			remainingWaiters = append(remainingWaiters, waiter)
		}
	}

	db.streamWaiters[key] = remainingWaiters
}

// Get stream length
func (db *DataBase) XLen(key string) int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	entry, exists := db.M[key]
	if !exists || !entry.IsStream() {
		return 0
	}

	return int64(len(entry.stream.Entries))
}

// Read range of entries
func (db *DataBase) XRange(key string, start, end string, count int) []StreamEntry {
	db.mu.RLock()
	defer db.mu.RUnlock()

	entry, exists := db.M[key]
	if !exists || !entry.IsStream() {
		return []StreamEntry{}
	}

	stream := entry.stream
	var results []StreamEntry

	for _, streamEntry := range stream.Entries {
		// Check start boundary (- means from beginning)
		if start != "-" && compareStreamIDs(streamEntry.ID, start) < 0 {
			continue
		}

		// Check end boundary (+ means to end)
		if end != "+" && compareStreamIDs(streamEntry.ID, end) > 0 {
			break
		}

		results = append(results, streamEntry)

		// Check count limit
		if count > 0 && len(results) >= count {
			break
		}
	}

	return results
}

// Read from stream starting after given ID
func (db *DataBase) XRead(keys []string, ids []string, count int) map[string][]StreamEntry {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := make(map[string][]StreamEntry)

	for i, key := range keys {
		if i >= len(ids) {
			continue
		}

		entry, exists := db.M[key]
		if !exists || !entry.IsStream() {
			continue
		}

		stream := entry.stream
		startID := ids[i]

		// Handle special ID "$" - means start from end (latest)
		if startID == "$" {
			if len(stream.Entries) > 0 {
				startID = stream.Entries[len(stream.Entries)-1].ID
			} else {
				continue
			}
		}

		var entries []StreamEntry
		for _, streamEntry := range stream.Entries {
			// Only include entries after the start ID
			if compareStreamIDs(streamEntry.ID, startID) > 0 {
				entries = append(entries, streamEntry)
				if count > 0 && len(entries) >= count {
					break
				}
			}
		}

		if len(entries) > 0 {
			result[key] = entries
		}
	}

	return result
}

func (db *DataBase) XReadBlocking(keys []string, ids []string, count int, blockMs int64) (map[string][]StreamEntry, error) {
	// First try non-blocking read
	result := db.XRead(keys, ids, count)
	if len(result) > 0 {
		return result, nil
	}

	// If no data and block time is 0, return immediately
	if blockMs == 0 {
		return result, nil
	}

	// Set up blocking read
	waiter := &StreamWaiter{
		Keys:     keys,
		IDs:      ids,
		Count:    count,
		Response: make(chan map[string][]StreamEntry, 1),
	}

	// Register waiter for all keys
	db.waiterMutex.Lock()
	if db.streamWaiters == nil {
		db.streamWaiters = make(map[string][]*StreamWaiter)
	}
	for _, key := range keys {
		db.streamWaiters[key] = append(db.streamWaiters[key], waiter)
	}
	db.waiterMutex.Unlock()

	// Setup timeout
	var timeout <-chan time.Time
	if blockMs > 0 {
		timeout = time.After(time.Duration(blockMs) * time.Millisecond)
	}

	// Wait for notification or timeout
	select {
	case result := <-waiter.Response:
		return result, nil
	case <-timeout:
		// Remove waiter on timeout
		db.removeWaiter(waiter, keys)
		return map[string][]StreamEntry{}, nil
	}
}

func (db *DataBase) removeWaiter(waiter *StreamWaiter, keys []string) {
	db.waiterMutex.Lock()
	defer db.waiterMutex.Unlock()

	for _, key := range keys {
		waiters := db.streamWaiters[key]
		var newWaiters []*StreamWaiter
		for _, w := range waiters {
			if w != waiter {
				newWaiters = append(newWaiters, w)
			}
		}
		db.streamWaiters[key] = newWaiters
	}
}
