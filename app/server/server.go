package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/arayofcode/codecrafters-kafka/app/protocol"
)

func parseRequest(conn net.Conn) (parsedRequest protocol.Request, err error) {
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

func sendResponse(conn net.Conn, res protocol.Response) (err error) {
	// Only the response body/ headers
	var bodyBuf bytes.Buffer
	err = binary.Write(&bodyBuf, binary.BigEndian, int32(res.CorrelationID))
	if err != nil {
		return fmt.Errorf("Error while writing correlation_id: %v", err)
	}

	// Add body to final message
	var messageBuf bytes.Buffer
	binary.Write(&messageBuf, binary.BigEndian, int32(bodyBuf.Len()))
	_, err = messageBuf.Write(bodyBuf.Bytes())
	if err != nil {
		return fmt.Errorf("Error while writing message: %v", err)
	}

	// Send message
	_, err = conn.Write(messageBuf.Bytes())
	if err != nil {
		return fmt.Errorf("Error while writing sending message: %v", err)
	}
	return
}

func sendInvalidVersionResponse(conn net.Conn, res protocol.Response) {
	// Only the response body/ headers
	var bodyBuf bytes.Buffer
	binary.Write(&bodyBuf, binary.BigEndian, int32(res.CorrelationID))
	binary.Write(&bodyBuf, binary.BigEndian, int16(protocol.UnsupportedVersionErrorCode))

	// Add body to final message
	var messageBuf bytes.Buffer
	binary.Write(&messageBuf, binary.BigEndian, int32(messageBuf.Len()))
	messageBuf.Write(bodyBuf.Bytes())

	// Send message
	conn.Write(messageBuf.Bytes())
}

func HandleConnection(conn net.Conn) {
	defer conn.Close()

	request, err := parseRequest(conn)
	if err != nil {
		fmt.Printf("Error in parsing request: %+v\n", err)
		return
	}

	var response protocol.Response
	response.CorrelationID = request.CorrelationID

	if !request.ValidApiVersion() {
		sendInvalidVersionResponse(conn, response)
		return
	}

	sendResponse(conn, response)
}
