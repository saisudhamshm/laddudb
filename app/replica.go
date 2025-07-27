package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

type ReplicationInfo struct {
	IsMaster         bool
	masterReplID     string
	masterReplOffset int64
	masterAddr       string
	replicas         []*net.Conn
	ReplOffset       int
}

func WriteReplInfo(replicationInfo ReplicationInfo, conn *net.Conn, w *RESPreader) {
	role := "master"
	if !replicationInfo.IsMaster {
		role = "slave"
	}
	info := fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d", role,
		replicationInfo.masterReplID,
		replicationInfo.masterReplOffset)
	w.Write(RespData{Type: BulkString, Str: info})
}

func handShake(masterAddr, slavePort string) (*net.Conn, error) {
	parts := strings.Split(masterAddr, " ")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid master address format. Expected '<IP> <PORT>', got '%s'", masterAddr)
	}

	ip := parts[0]
	port := parts[1]

	// Form the address string in the format expected by net.Dial
	address := fmt.Sprintf("%s:%s", ip, port)

	// Attempt to establish TCP connection with master
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	w := NewRESPreader(conn)
	buf := make([]byte, 2048)

	w.Write(RespData{Type: Array, Array: []RespData{{Type: BulkString, Str: "PING"}}})
	w.writer.Flush()
	conn.Read(buf)
	// fmt.Println(string(buf))

	w.Write(RespData{Type: Array, Array: []RespData{
		{Type: BulkString, Str: "REPLCONF"},
		{Type: BulkString, Str: "listening-port"},
		{Type: BulkString, Str: slavePort}, // Replace with actual slave port
	}})
	w.writer.Flush()
	conn.Read(buf)
	// fmt.Println(string(buf))

	w.Write(RespData{Type: Array, Array: []RespData{
		{Type: BulkString, Str: "REPLCONF"},
		{Type: BulkString, Str: "capa"},
		{Type: BulkString, Str: "psync2"}, // Replace with actual slave port
	}})
	w.writer.Flush()
	conn.Read(buf)
	// fmt.Println(string(buf))

	w.Write(RespData{Type: Array, Array: []RespData{
		{Type: BulkString, Str: "PSYNC"},
		{Type: BulkString, Str: "?"},
		{Type: BulkString, Str: "-1"}, // Replace with actual slave port
	}})
	w.writer.Flush()
	conn.Read(buf)

	return &conn, nil
}

func addReplica(db *DataBase, conn *net.Conn) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	db.replicationInfo.replicas = append(db.replicationInfo.replicas, conn)
	log.Println("Replica added successfully")
	return nil
}
