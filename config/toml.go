package config

import (
	"bytes"
	"encoding/hex"
	"path/filepath"
	"strings"
	"text/template"

	_ "embed"

	toml "github.com/pelletier/go-toml/v2"

	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtos "github.com/cometbft/cometbft/internal/os"
)

// DefaultDirPerm is the default permissions used when creating directories.
const DefaultDirPerm = 0o700

var configTemplate *template.Template

func init() {
	var err error
	tmpl := template.New("configFileTemplate").Funcs(template.FuncMap{
		"StringsJoin": strings.Join,
	})
	if configTemplate, err = tmpl.Parse(defaultConfigTemplate); err != nil {
		panic(err)
	}
}

// ****** these are for production settings *********** //

// EnsureFilesystem creates the root, config and data directories if they don't exist.
//
// This method returns an error if any of the operations fail.
//
// This method is used when EnsureRoot is called, to setup the filesystem.
func EnsureFilesystem(rootDir string) error {
	if err := cmtos.EnsureDir(rootDir, DefaultDirPerm); err != nil {
		return err
	}
	if err := cmtos.EnsureDir(filepath.Join(rootDir, DefaultConfigDir), DefaultDirPerm); err != nil {
		return err
	}
	if err := cmtos.EnsureDir(filepath.Join(rootDir, DefaultDataDir), DefaultDirPerm); err != nil {
		return err
	}

	return nil
}

// EnsureConfig creates the default configuration file using a [Config] instance.
//
// This method is used when EnsureRoot is called, to setup default configuration.
func EnsureConfig(rootDir string, conf *Config) {
	configFilePath := filepath.Join(rootDir, defaultConfigFilePath)

	// Write default config file if missing.
	if !cmtos.FileExists(configFilePath) {
		WriteConfigFile(configFilePath, conf)
	}
}

// EnsureConfigFile creates a "config.toml" file using a root directory and [Config] instance.
//
// This method is used to create separate configuration files per replicated chain (multiplex).
func EnsureConfigFile(rootDir string, conf *Config) {
	configFilePath := filepath.Join(rootDir, "config.toml")

	// Write scoped config file if missing.
	if !cmtos.FileExists(configFilePath) {
		WriteConfigFile(configFilePath, conf)
	}
}

// EnsureRoot creates the filesystem structure and necessary configuration files
// or panics if it fails.
//
// Updated to execute separate filesystem and configuration existence checks.
func EnsureRoot(rootDir string) {
	if err := EnsureFilesystem(rootDir); err != nil {
		panic(err.Error())
	}

	EnsureConfig(rootDir, DefaultConfig())
}

// EnsureRootMultiplex creates the scoped data directories if they don't exist,
// and panics if it fails.
//
// FIXME: should use ScopeRegistry implementation and moved to multiplex/
func EnsureRootMultiplex(rootDir string, config *BaseConfig) {
	// Storage is located in scopes subfolders per each user
	// Uses one subfolder by user and one subfolder by scope
	for _, userAddress := range config.GetAddresses() {
		for _, scope := range config.UserScopes[userAddress] {
			// Create scopeID, then SHA256 and create 8-bytes fingerprint
			scopeId := strings.Join([]string{userAddress, scope}, ":")
			fingerprint := tmhash.Sum([]byte(scopeId))[:8]

			// The folder name is the hex representation of the fingerprint
			folderName := strings.ToUpper(hex.EncodeToString(fingerprint))

			confPath := filepath.Join(rootDir, DefaultConfigDir, userAddress, folderName)
			dataPath := filepath.Join(rootDir, DefaultDataDir, userAddress, folderName)

			if err := cmtos.EnsureDir(confPath, DefaultDirPerm); err != nil {
				panic(err.Error())
			}

			if err := cmtos.EnsureDir(dataPath, DefaultDirPerm); err != nil {
				panic(err.Error())
			}
		}
	}
}

// XXX: this func should probably be called by cmd/cometbft/commands/init.go
// alongside the writing of the genesis.json and priv_validator.json.
func writeDefaultConfigFile(configFilePath string) {
	WriteConfigFile(configFilePath, DefaultConfig())
}

// WriteConfigFile renders config using the template and writes it to configFilePath.
func WriteConfigFile(configFilePath string, config *Config) {
	var buffer bytes.Buffer

	if err := configTemplate.Execute(&buffer, config); err != nil {
		panic(err)
	}

	cmtos.MustWriteFile(configFilePath, buffer.Bytes(), 0o644)
}

// ReadConfigFile unmarshals a toml configuration file using pelletier/go-toml.
func ReadConfigFile(configFilePath string) *Config {
	var conf Config
	confBytes := cmtos.MustReadFile(configFilePath)

	// TODO(midas): should not dismiss unmarshaling errors (e.g. time.Duration)
	toml.Unmarshal(confBytes, &conf)
	return &conf
}

// Note: any changes to the comments/variables/mapstructure
// must be reflected in the appropriate struct in config/config.go.
//
//go:embed config.toml.tpl
var defaultConfigTemplate string
