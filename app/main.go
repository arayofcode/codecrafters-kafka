package main

import (
	"fmt"
	"net"
	"os"

	"github.com/arayofcode/codecrafters-kafka/app/apikeys/apiversions"
	"github.com/arayofcode/codecrafters-kafka/app/protocol"
	"github.com/arayofcode/codecrafters-kafka/app/server"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	srv := server.NewServer()
	srv.RegisterHandler(protocol.ApiVersionsRequestKey, protocol.ApiVersion(4), apiversions.HandleApiVersions)

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Printf("Failed to bind to port 9092: %+v", err)
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go srv.HandleConnection(conn)
	}
}
