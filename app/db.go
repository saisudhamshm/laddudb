package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/parser"
)

type DataBase struct {
	M               map[string]DBentry
	dir             string
	dbfilename      string
	port            string
	rdbVersion      int
	replicationInfo ReplicationInfo
}

type DBentry struct {
	val       string
	expiresAt int64
	timestamp int64
}

func (db *DataBase) Addex(key string, val string, expiresAt int64) {
	db.M[key] = DBentry{val, expiresAt, time.Now().UnixMilli()}
}

func (db *DataBase) Add(key string, val string) {
	db.M[key] = DBentry{val, -1, time.Now().UnixMilli()}
}

func (db *DataBase) Get(key string) *string {
	if entry, ok := db.M[key]; ok {
		if entry.expiresAt == -1 || entry.expiresAt+entry.timestamp >= time.Now().UnixMilli() {
			return &entry.val
		} else {
			delete(db.M, key)
			return nil
		}
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
		},
	}
	return db
}

func (db *DataBase) init() {
	if !db.replicationInfo.IsMaster {
		handShake(db.replicationInfo.masterAddr, db.port)
	}
	if _, err := os.Stat(db.dir + "/" + db.dbfilename); err == nil {
		err := db.LoadRDB()
		if err != nil {
			fmt.Printf("Error loading RDB file: %v\n", err)
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
	err = enc.WriteDBHeader(0, uint64(len(db.M)), 0) // database 0, key count, TTL count
	if err != nil {
		return fmt.Errorf("failed to write database header: %w", err)
	}

	for key, entry := range db.M {
		if entry.expiresAt != -1 && entry.expiresAt+entry.timestamp < now {
			continue // Skip expired keys
		}

		// Set expiry if exists
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
	if err := enc.WriteDBHeader(0, 0, 0); err != nil {
		return fmt.Errorf("failed to write db header: %v", err)
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
	if _, err := conn.Write(message.Bytes()); err != nil {
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
		// Only process string types for string-string key-value pairs
		if o.GetType() == parser.StringType {
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

			// Store the key-value pair in the map

		}

		// Continue processing all objects
		return true
	})

	if err != nil {
		return fmt.Errorf("failed to parse RDB file: %w", err)
	}

	return nil
}
