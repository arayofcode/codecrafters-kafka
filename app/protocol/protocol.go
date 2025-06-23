package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

type (
	CorrelationId int32
	ApiKey        int16
	ApiVersion    int16
	ErrorCode     int16
	MessageSize   int32
	ThrottleTime  int32
)

const (
	NoErrorCode                 ErrorCode = 0
	UnsupportedVersionErrorCode ErrorCode = 35
)

const (
	ProduceRequestKey      ApiKey = 0
	FetchRequestKey        ApiKey = 1
	MetadataRequestKey     ApiKey = 3
	ApiVersionsRequestKey  ApiKey = 18
	CreateTopicsRequestKey ApiKey = 19
)

type ApiVersionRange struct {
	MinVersion ApiVersion
	MaxVersion ApiVersion
}

var SupportedVersions = map[ApiKey]ApiVersionRange{
	MetadataRequestKey:    {MinVersion: 0, MaxVersion: 4},
	ProduceRequestKey:     {MinVersion: 5, MaxVersion: 11},
	FetchRequestKey:       {MinVersion: 0, MaxVersion: 16},
	ApiVersionsRequestKey: {MinVersion: 0, MaxVersion: 4},
}

type Request struct {
	RequestApiKey     ApiKey
	RequestApiVersion ApiVersion
	CorrelationID     CorrelationId
	ClientID          *string
	TagBuffer         []byte
	RequestData       RequestData
}

type ErrorResponse struct {
	ErrorCode ErrorCode
}

type Response struct {
	CorrelationID CorrelationId
	ErrorResponse
}

type RequestData interface {
	Parse()
}

type ResponseData interface {
	Encode(w io.Writer) error
}

type RouterKey struct {
	ApiKey     ApiKey
	ApiVersion ApiVersion
}

type HandlerFunc func(req *Request, body io.Reader) (ResponseData, error)

func (req Request) ValidApiVersion() bool {
	apiKey := req.RequestApiKey
	version := req.RequestApiVersion
	versionRange, supported := SupportedVersions[apiKey]
	if supported && version >= versionRange.MinVersion && version <= versionRange.MaxVersion {
		return true
	}
	return false
}

func (e *ErrorResponse) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, e.ErrorCode); err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}
	return nil
}
