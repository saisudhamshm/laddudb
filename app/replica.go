package main

import (
	"fmt"
	"net"
)

type ReplicationInfo struct {
	IsMaster         bool
	masterReplID     string
	masterReplOffset int
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
