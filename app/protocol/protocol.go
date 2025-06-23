package protocol

import "io"

const (
	NoErrorCode                 int16 = 0
	UnsupportedVersionErrorCode int16 = 35
)

const (
	ProduceRequestKey      int16 = 0
	FetchRequestKey        int16 = 1
	MetadataRequestKey     int16 = 3
	ApiVersionsRequestKey  int16 = 18
	CreateTopicsRequestKey int16 = 19
)

type ApiVersionRange struct {
	MinVersion int16
	MaxVersion int16
}

var SupportedVersions = map[int16]ApiVersionRange{
	MetadataRequestKey:    {MinVersion: 0, MaxVersion: 4},
	ProduceRequestKey:     {MinVersion: 5, MaxVersion: 11},
	FetchRequestKey:       {MinVersion: 0, MaxVersion: 3},
	ApiVersionsRequestKey: {MinVersion: 0, MaxVersion: 4},
}

type Request struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationID     int32
	ClientID          *string
	TagBuffer         []byte
	RequestData       RequestData
}

type Response struct {
	CorrelationID int32
	ErrorCode     int16
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
