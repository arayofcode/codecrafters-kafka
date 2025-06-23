package protocol

import "io"

type (
	CorrelationId     int32
	RequestApiKey     int16
	RequestApiVersion int16
	ErrorCode         int16
)

const (
	NoErrorCode                 ErrorCode = 0
	UnsupportedVersionErrorCode ErrorCode = 35
)

const (
	ProduceRequestKey      RequestApiKey = 0
	FetchRequestKey        RequestApiKey = 1
	MetadataRequestKey     RequestApiKey = 3
	ApiVersionsRequestKey  RequestApiKey = 18
	CreateTopicsRequestKey RequestApiKey = 19
)

type ApiVersionRange struct {
	MinVersion RequestApiVersion
	MaxVersion RequestApiVersion
}

var SupportedVersions = map[RequestApiKey]ApiVersionRange{
	MetadataRequestKey:    {MinVersion: 0, MaxVersion: 4},
	ProduceRequestKey:     {MinVersion: 5, MaxVersion: 11},
	FetchRequestKey:       {MinVersion: 0, MaxVersion: 3},
	ApiVersionsRequestKey: {MinVersion: 0, MaxVersion: 4},
}

type Request struct {
	RequestApiKey     RequestApiKey
	RequestApiVersion RequestApiVersion
	CorrelationID     CorrelationId
	ClientID          *string
	TagBuffer         []byte
	RequestData       RequestData
}

type Response struct {
	CorrelationID CorrelationId
	ErrorCode     ErrorCode
}

type RequestData interface {
	Parse()
}

type ResponseData interface {
	Encode(w io.Writer)
}

func (req Request) ValidApiVersion() bool {
	apiKey := req.RequestApiKey
	version := req.RequestApiVersion
	versionRange, supported := SupportedVersions[apiKey]
	if supported && version >= versionRange.MinVersion && version <= versionRange.MaxVersion {
		return true
	}
	return false
}
