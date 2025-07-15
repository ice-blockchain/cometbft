package helpers

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/ice-blockchain/cometbft/crypto/tmhash"
)

const (
	// Defines the prefix used for ChainID, e.g. `mx-chain...`
	DefaultMultiplexPrefix = "mx-chain"

	// Defines the fields separator for ChainID.
	DefaultFieldSeparator = "-"

	// Defines the size of fingerprints in bytes.
	DefaultFingerprintSize = 8
)

// -----------------------------------------------------------------------------
// ExtendedChainID
//
// ExtendedChainID defines an interface to handle ChainID values that contain
// special fields, e.g. a user address or an arbitrary fingerprint.
type ExtendedChainID interface {
	fmt.Stringer

	// GetSeparator should return a fields separator.
	GetSeparator() string

	// GetUserAddress should return the user address in hexadecimal as present
	// in the formatted chain identifier.
	GetUserAddress() string

	// GetFingerprint should return the fingerprint in hexadecimal as present
	// in the formatted chain identifier.
	// The fingerprint is validated for its' size, the content can be arbitrary.
	GetFingerprint() string

	// Format should return a formatted chain identifier that contains the
	// above listed fields: user address and fingerprint.
	Format() string
}

var _ ExtendedChainID = (*extendedChainID)(nil)

// -----------------------------------------------------------------------------
// extendedChainID
//
// extendedChainID is unexported to prevent using invalid values.
type extendedChainID struct {
	// Prefix contains a chain identifier prefix, always "mx-chain".
	Prefix string

	// UserAddress contains a user address in hexadecimal notation (20 bytes).
	UserAddress string

	// Fingerprints contains a fingerprint in hexadecimal notation (8 bytes).
	Fingerprint string
}

// GetSeparator returns the fields separator constant.
// GetSeparator implements ExtendedChainID.
func (c *extendedChainID) GetSeparator() string {
	return DefaultFieldSeparator
}

// GetUserAddress returns the 20 bytes user address in hexadecimal format.
//
// Note that the size of the user address must match the 20 bytes hexadecimal
// notation defined with [crypto.tmhash.TruncatedSize] and represents a user
// address which can be unmarshalled into a [crypto.Address].
//
// GetUserAddress implements ExtendedChainID.
func (c *extendedChainID) GetUserAddress() string {
	return c.UserAddress
}

// GetFingerprint returns the 8 bytes fingerprint in hexadecimal format.
//
// Note that the fingerprint is validated only for its' size, thus the content
// may be arbitrary and is never parsed for validaty checks.
//
// GetFingerprint implements ExtendedChainID.
func (c *extendedChainID) GetFingerprint() string {
	return c.Fingerprint
}

// Format a formatted chain identifier that contains a user address and an
// arbitrary 8 bytes fingerprint.
// Format implements ExtendedChainID.
func (c *extendedChainID) Format() string {
	// Extended ChainID contains prefix, user address and fingerprint
	return strings.Join([]string{
		c.Prefix,
		c.UserAddress,
		c.Fingerprint,
	}, c.GetSeparator())
}

// String returns a formatted chain identifier.
// String implements fmt.Stringer.
func (c *extendedChainID) String() string {
	return c.Format()
}

//------------------------------------------------------------
// Providers

// NewExtendedChainID creates an extended chain identifier which contains
// a user address and a fingerprint.
//
// This method expects the userAddress and fingerprint parameters to contain
// hexadecimal payloads. It will check for correct size match for both the
// user address (20 bytes) and the arbitrary fingerprint (8 bytes).
func NewExtendedChainID(userAddress, fingerprint string) ExtendedChainID {
	if len(userAddress) == 0 || len(fingerprint) == 0 {
		return nil
	}

	// Validate the size and format
	fp, err := hex.DecodeString(fingerprint)
	if err != nil || len(fp) != DefaultFingerprintSize {
		return nil
	}

	// Validate the size and format
	address, err := hex.DecodeString(userAddress)
	if err != nil || len(address) != tmhash.TruncatedSize {
		return nil
	}

	return &extendedChainID{
		Prefix:      DefaultMultiplexPrefix,
		UserAddress: userAddress,
		Fingerprint: fingerprint,
	}
}

// NewExtendedChainIDFromString extracts fields of a chain identifier, notably
// the user address and fingerprints which must both be present.
//
// The prefix is ignored to prevent using different prefixes than `mx-chain`.
func NewExtendedChainIDFromString(chainID string) ExtendedChainID {
	if len(chainID) == 0 {
		return nil
	}

	// Extract using regexp
	extractor := regexp.MustCompile(`(.*)\-([A-F0-9]+)\-([A-F0-9]+)`)
	matches := extractor.FindStringSubmatch(chainID)

	if len(matches) == 0 || len(matches) < 4 {
		return nil
	}

	// address, fingerprint
	return NewExtendedChainID(matches[2], matches[3])
}
