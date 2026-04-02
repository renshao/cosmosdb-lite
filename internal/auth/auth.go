package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
)

// WellKnownKey is the default CosmosDB emulator master key.
const WellKnownKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

// decodedKey is the base64-decoded master key.
var decodedKey []byte

func init() {
	var err error
	decodedKey, err = base64.StdEncoding.DecodeString(WellKnownKey)
	if err != nil {
		panic(fmt.Sprintf("failed to decode well-known key: %v", err))
	}
}

// ValidateAuth validates the CosmosDB Authorization header.
// verb: HTTP method (GET, POST, etc.)
// resourceType: "dbs", "colls", "docs"
// resourceLink: e.g., "dbs/mydb/colls/mycoll"
// date: x-ms-date header value
// authHeader: the Authorization header value
func ValidateAuth(verb, resourceType, resourceLink, date, authHeader string) error {
	if authHeader == "" {
		return fmt.Errorf("missing Authorization header")
	}

	// URL-decode the auth header if it's URL-encoded (SDK sends encoded,
	// web UI sends decoded). Detect by checking for '%' which indicates encoding.
	decoded := authHeader
	if strings.Contains(authHeader, "%") {
		var err error
		decoded, err = url.QueryUnescape(authHeader)
		if err != nil {
			return fmt.Errorf("invalid Authorization header encoding: %w", err)
		}
	}

	// Parse type=master&ver=1.0&sig=...
	parts := map[string]string{}
	for _, segment := range strings.Split(decoded, "&") {
		kv := strings.SplitN(segment, "=", 2)
		if len(kv) == 2 {
			parts[kv[0]] = kv[1]
		}
	}

	if parts["type"] != "master" {
		return fmt.Errorf("unsupported auth type: %s (only master key supported)", parts["type"])
	}
	if parts["ver"] != "1.0" {
		return fmt.Errorf("unsupported auth version: %s", parts["ver"])
	}

	sig := parts["sig"]
	if sig == "" {
		return fmt.Errorf("missing signature in Authorization header")
	}

	// Build string-to-sign
	stringToSign := strings.ToLower(verb) + "\n" +
		strings.ToLower(resourceType) + "\n" +
		resourceLink + "\n" +
		strings.ToLower(date) + "\n" +
		"" + "\n"

	// Compute HMAC-SHA256
	mac := hmac.New(sha256.New, decodedKey)
	mac.Write([]byte(stringToSign))
	expectedSig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(sig), []byte(expectedSig)) {
		return fmt.Errorf("invalid signature (verb=%q resType=%q resLink=%q date=%q clientSig=%q expectedSig=%q)",
			verb, resourceType, resourceLink, date, sig, expectedSig)
	}

	return nil
}

// GenerateAuth generates a CosmosDB Authorization token (for the web UI).
func GenerateAuth(verb, resourceType, resourceLink, date string) string {
	stringToSign := strings.ToLower(verb) + "\n" +
		strings.ToLower(resourceType) + "\n" +
		resourceLink + "\n" +
		strings.ToLower(date) + "\n" +
		"" + "\n"

	mac := hmac.New(sha256.New, decodedKey)
	mac.Write([]byte(stringToSign))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	token := fmt.Sprintf("type=master&ver=1.0&sig=%s", sig)
	return url.QueryEscape(token)
}
