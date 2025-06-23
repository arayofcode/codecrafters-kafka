package apiversions

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/arayofcode/codecrafters-kafka/app/protocol"
)

type ApiVersionsResponse struct {
	ErrorCode    protocol.ErrorCode
	ApiVersions  []ApiVersion
	ThrottleTime protocol.ThrottleTime
}

type ApiVersion struct {
	ApiKey protocol.ApiKey
	protocol.ApiVersionRange
}

func (a *ApiVersion) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, a.ApiKey); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, a.MinVersion); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, a.MaxVersion); err != nil {
		return err
	}
	// Tag buffer
	if err := binary.Write(w, binary.BigEndian, uint8(0)); err != nil {
		return err
	}
	return nil
}

func (a *ApiVersionsResponse) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, a.ErrorCode); err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}

	if err := binary.Write(w, binary.BigEndian, int8(len(a.ApiVersions)+1)); err != nil {
		return fmt.Errorf("failed to encode array length: %w", err)
	}

	for _, apiVersion := range a.ApiVersions {
		if err := apiVersion.Encode(w); err != nil {
			return fmt.Errorf("failed to encode api version: %w", err)
		}
	}

	// Throttle time
	if err := binary.Write(w, binary.BigEndian, a.ThrottleTime); err != nil {
		return fmt.Errorf("failed to encode throttle time: %w", err)
	}

	// Tag buffer
	if err := binary.Write(w, binary.BigEndian, uint8(0)); err != nil {
		return err
	}
	return nil
}

func HandleApiVersions(req *protocol.Request, body io.Reader) (protocol.ResponseData, error) {
	apiVersions := make([]ApiVersion, 0, len(protocol.SupportedVersions))

	for apiKey, versionRange := range protocol.SupportedVersions {
		apiVersions = append(apiVersions, ApiVersion{
			ApiKey:          apiKey,
			ApiVersionRange: versionRange,
		})
	}

	responseBody := &ApiVersionsResponse{
		ErrorCode:    protocol.NoErrorCode,
		ThrottleTime: 0,
		ApiVersions:  apiVersions,
	}

	return responseBody, nil
}
