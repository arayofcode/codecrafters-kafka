package main

func (req Request) ValidApiVersion() bool {
	version := req.RequestApiVersion
	versionRange, supported := SupportedVersions[version]
	if supported && version >= versionRange.MinVersion && version <= versionRange.MaxVersion {
		return true
	}
	return false
}
