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
}
type DataType int

const (
	StringType DataType = iota
	ListType
	// Add other types as needed (HashType, SetType, etc.)
)

type DBentry struct {
	dataType  DataType
	val       string
	list      []string
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
	db.M[key] = DBentry{StringType, val, nil, expiresAt, time.Now().UnixMilli()}
}

func (db *DataBase) Add(key string, val string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.M[key] = DBentry{StringType, val, nil, -1, time.Now().UnixMilli()}
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
		db.M[key] = DBentry{StringType, "1", nil, -1, time.Now().UnixMilli()}
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

		default:
			log.Println("Received unknown command from master:", cmd.cmd)

		}

	}
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

			}
			// Set expiry if exists

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
					// Key has expired, skip it
					return true
				}
				db.M[str.Key] = DBentry{
					val:       string(str.Value),
					timestamp: time.Now().UnixMilli(),
					expiresAt: time.Now().UnixMilli() - str.GetExpiration().UnixMilli(),
				}
			} else {
				db.M[str.Key] = DBentry{
					val:       string(str.Value),
					timestamp: time.Now().UnixMilli(),
					expiresAt: -1,
				}
			}
		case parser.ListType:
			listObj := o.(*parser.ListObject)
			if listObj.GetExpiration() != nil {
				expirationTime := *listObj.GetExpiration()
				if time.Now().After(expirationTime) {
					return true // Skip expired key
				}
			}
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

		// Continue processing all objects
		return true
	})

	if err != nil {
		return fmt.Errorf("failed to parse RDB file: %w", err)
	}

	return nil
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
