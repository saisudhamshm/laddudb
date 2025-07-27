package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
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
		log.Println("Failed to bind to port" + port)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	r := NewRESPreader(conn)
	clientConn := ClientConn{conn: conn, isTransaction: false}
	for {
		val, _, err := r.Read()
		if err != nil {
			conn.Close()
			return
		}
		cmd, er := parseCmd(val)
		log.Printf("Received command: %s", cmd.cmd)
		if er != nil {
			log.Println("Error parsing command: ", er)
		}
		handleCommand(cmd, r, &clientConn)
	}

}
