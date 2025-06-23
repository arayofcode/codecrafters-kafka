package main

// For given request key, check if the version present in request is supported
func (req Request) ValidApiVersion() bool {
	apiKey := req.RequestApiKey
	version := req.RequestApiVersion
	versionRange, supported := SupportedVersions[apiKey]
	if supported && version >= versionRange.MinVersion && version <= versionRange.MaxVersion {
		return true
	}
	return false
}
