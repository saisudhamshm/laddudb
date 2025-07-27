package main

func handleMultiCommand(cmd Command, clientConn *ClientConn) RespData {
	if len(cmd.args) != 0 {
		return RespData{Type: Error, Str: "ERR wrong number of arguments for 'multi' command"}
	}
	if clientConn.isTransaction {
		return RespData{Type: Error, Str: "ERR MULTI calls can not be nested"}
	}

	clientConn.isTransaction = true
	return RespData{Type: SimpleString, Str: "BEGIN"}
}

func handleExecCommand(clientConn *ClientConn) RespData {
	if !clientConn.isTransaction {
		return RespData{Type: Error, Str: "ERR EXEC without MULTI"}
	}
	var results []RespData
	for _, queuedCmd := range clientConn.transactionQueue {
		// Temporarily disable transaction mode to execute commands
		result := executeCommand(queuedCmd, clientConn, true)
		results = append(results, result)
	}

	// Reset transaction state
	clientConn.isTransaction = false
	clientConn.transactionQueue = nil

	return RespData{Type: Array, Array: results}
}

func handleDiscardCommand(clientConn *ClientConn) RespData {
	if !clientConn.isTransaction {
		return RespData{Type: Error, Str: "ERR DISCARD without MULTI"}
	}

	clientConn.isTransaction = false
	clientConn.transactionQueue = nil
	return RespData{Type: SimpleString, Str: "OK"}
}
