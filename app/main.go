package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var db *DataBase

func main() {
	var (
		dir        string
		dbfilename string
		port       string
		replicaOf  string
	)
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	flag.StringVar(&dir, "dir", "~/redisdb", "location of database")
	flag.StringVar(&dbfilename, "dbfilename", "data.rdb", "name of rdb file")
	flag.StringVar(&port, "port", "6379", "port number for the server")
	flag.StringVar(&replicaOf, "replicaof", "", "address of master server")
	flag.Parse()
	fmt.Println("Logs from your program will appear here!")
	db = NewDatabase(dir, dbfilename, port, replicaOf)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Saving database and shutting down...")
		if err := db.SaveRDB(); err != nil {
			fmt.Printf("Error saving RDB file: %v\n", err)
		}
		os.Exit(0)
	}()

	// Start periodic RDB saving
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := db.SaveRDB(); err != nil {
				fmt.Printf("Error during periodic RDB save: %v\n", err)
			}
		}
	}()

	// Expand home directory if needed
	if dir[:2] == "~/" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			fmt.Println("Failed to get home directory:", err)
			os.Exit(1)
		}
		dir = filepath.Join(homeDir, dir[2:])
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Println("Failed to create database directory:", err)
		os.Exit(1)
	}

	db.init()

	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port" + port)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	r := NewRESPreader(conn)
	for {
		val, err := r.Read()
		if err != nil {
			conn.Close()
			return
		}
		cmd, er := parseCmd(val)
		if er != nil {
			fmt.Println("Error parsing command: ", er)
		}

		handleCommand(cmd, r, conn)
	}

}

func handleCommand(cmd Command, r *RESPreader, conn net.Conn) {
	if strings.ToLower(cmd.cmd) == "ping" {
		r.Write(RespData{Type: SimpleString, Str: "PONG"})
	} else if strings.ToLower(cmd.cmd) == "echo" {
		r.Write(RespData{Type: BulkString, Str: cmd.args[0]})
	} else if strings.ToLower(cmd.cmd) == "set" {
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

	} else if strings.ToLower(cmd.cmd) == "get" {
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
	} else if strings.ToLower(cmd.cmd) == "save" {
		if err := db.SaveRDB(); err != nil {
			r.Write(RespData{Type: Error, Str: fmt.Sprintf("ERR %v", err)})
		} else {
			r.Write(RespData{Type: SimpleString, Str: "OK"})
		}
	} else if strings.ToLower(cmd.cmd) == "config" {
		if strings.ToLower(cmd.args[0]) == "get" {
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
		} else if strings.ToLower(cmd.args[0]) == "set" {
			if cmd.args[1] == "dir" {
				db.dir = cmd.args[2]
				r.Write(RespData{Type: SimpleString, Str: "OK"})
			} else if cmd.args[1] == "dbfilename" {
				db.dbfilename = cmd.args[2]
				r.Write(RespData{Type: SimpleString, Str: "OK"})
			}
		}
	} else if strings.ToLower(cmd.cmd) == "keys" {
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
	} else if strings.ToLower(cmd.cmd) == "info" {
		WriteReplInfo(db.replicationInfo, &conn, r)
	} else if strings.ToLower(cmd.cmd) == "replconf" {
		r.Write(RespData{Type: SimpleString, Str: "OK"})
	} else if strings.ToLower(cmd.cmd) == "psync" {
		r.Write(RespData{Type: SimpleString, Str: fmt.Sprintf("FULLRESYNC %s %d",
			db.replicationInfo.masterReplID, db.replicationInfo.masterReplOffset)})
		sendEmptyRDB(conn)
	} else {
		r.Write(RespData{Type: Error, Str: "ERR unknown command '" + cmd.cmd + "'"})
	}
}
