package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type Response struct {
	MessageSize   int32
	CorrelationID int32
	FinalMessage  bytes.Buffer
}

func handleConnection(request net.Conn) {
	defer request.Close()
	var response Response
	response.CorrelationID = 7
	response.MessageSize = 4
	binary.Write(&response.FinalMessage, binary.BigEndian, int32(response.MessageSize))
	binary.Write(&response.FinalMessage, binary.BigEndian, int32(response.CorrelationID))
	request.Write(response.FinalMessage.Bytes())
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		req, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(req)
	}
}
