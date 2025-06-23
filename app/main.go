package main

import (
	"fmt"
	"net"
	"os"

	"github.com/arayofcode/codecrafters-kafka/app/server"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Printf("Failed to bind to port 9092: %+v", err)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go server.HandleConnection(conn)
	}
}
