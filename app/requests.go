package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Request struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationID     int32
	ClientID          *string
	TagBuffer         []byte
}

type Response struct {
	MessageSize   int32
	CorrelationID int32
}

func ParseRequest(conn net.Conn) (parsedRequest Request, err error) {
	var messageSize int32
	if err = binary.Read(conn, binary.BigEndian, &messageSize); err != nil {
		return
	}

	messageData := make([]byte, messageSize)
	if _, err = io.ReadFull(conn, messageData); err != nil {
		return
	}
	reqReader := bytes.NewReader(messageData)

	if err = binary.Read(reqReader, binary.BigEndian, &parsedRequest.RequestApiKey); err != nil {
		return
	}

	if err = binary.Read(reqReader, binary.BigEndian, &parsedRequest.RequestApiVersion); err != nil {
		return
	}

	if err = binary.Read(reqReader, binary.BigEndian, &parsedRequest.CorrelationID); err != nil {
		return
	}
	return parsedRequest, nil
}

func (res Response) GetFinalMessage() []byte {
	var messageBuf bytes.Buffer
	binary.Write(&messageBuf, binary.BigEndian, int32(res.MessageSize))
	binary.Write(&messageBuf, binary.BigEndian, int32(res.CorrelationID))
	return messageBuf.Bytes()
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	request, err := ParseRequest(conn)
	if err != nil {
		fmt.Printf("Error in parsing request: %+v\n", err)
		return
	}

	var response Response
	response.CorrelationID = request.CorrelationID
	response.MessageSize = 0

	conn.Write(response.GetFinalMessage())
}
