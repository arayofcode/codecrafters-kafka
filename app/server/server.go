package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/arayofcode/codecrafters-kafka/app/protocol"
)

func parseRequest(conn net.Conn) (*protocol.Request, io.Reader, error) {
	var messageSize protocol.MessageSize
	if err := binary.Read(conn, binary.BigEndian, &messageSize); err != nil {
		return nil, nil, fmt.Errorf("failed to read message size: %w", err)
	}

	messageData := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, messageData); err != nil {
		return nil, nil, fmt.Errorf("failed to read message data: %w", err)
	}
	reqReader := bytes.NewReader(messageData)

	var parsedRequest protocol.Request
	if err := binary.Read(reqReader, binary.BigEndian, &parsedRequest.RequestApiKey); err != nil {
		return nil, nil, err
	}

	if err := binary.Read(reqReader, binary.BigEndian, &parsedRequest.RequestApiVersion); err != nil {
		return nil, nil, err
	}

	if err := binary.Read(reqReader, binary.BigEndian, &parsedRequest.CorrelationID); err != nil {
		return nil, nil, err
	}

	// Send remaining buffer back for handler to parse body
	return &parsedRequest, reqReader, nil
}

func sendResponse(conn net.Conn, correlationID protocol.CorrelationId, body protocol.ResponseData) (err error) {
	var payloadBuf bytes.Buffer
	if err = binary.Write(&payloadBuf, binary.BigEndian, correlationID); err != nil {
		return fmt.Errorf("failed to write correlation id: %w", err)
	}

	if body != nil {
		if err = body.Encode(&payloadBuf); err != nil {
			return fmt.Errorf("failed to write encode response body: %w", err)
		}
	}

	var messageBuf bytes.Buffer
	if err = binary.Write(&messageBuf, binary.BigEndian, int32(payloadBuf.Len())); err != nil {
		return fmt.Errorf("failed to write response message size: %w", err)
	}

	if _, err = messageBuf.Write(payloadBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write response message: %w", err)
	}

	_, err = conn.Write(messageBuf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write final message to connection: %w", err)
	}

	return nil
}

type Server struct {
	handlers map[protocol.RouterKey]protocol.HandlerFunc
}

func (r *Server) RegisterHandler(key protocol.ApiKey, version protocol.ApiVersion, handler protocol.HandlerFunc) {
	r.handlers[protocol.RouterKey{ApiKey: key, ApiVersion: version}] = handler
}

func NewServer() *Server {
	return &Server{
		handlers: make(map[protocol.RouterKey]protocol.HandlerFunc),
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()

	request, body, err := parseRequest(conn)
	if err != nil {
		fmt.Printf("error in parsing request: %+v\n", err)
		return
	}

	if !request.ValidApiVersion() {
		responseBody := &protocol.ErrorResponse{ErrorCode: protocol.UnsupportedVersionErrorCode}
		sendResponse(conn, request.CorrelationID, responseBody)
		return
	}

	key := protocol.RouterKey{
		ApiKey:     request.RequestApiKey,
		ApiVersion: request.RequestApiVersion,
	}
	handlerFunction, found := s.handlers[key]
	if !found {
		fmt.Printf("no handler found for key: %+v\n", key)
		return
	}

	responseData, err := handlerFunction(request, body)
	if err != nil {
		fmt.Printf("handler for key %+v returned an error: %v\n", key, err)
		return
	}

	fmt.Println(request.CorrelationID)

	if err := sendResponse(conn, request.CorrelationID, responseData); err != nil {
		fmt.Printf("failed to send response: %v\n", err)
	}
}
