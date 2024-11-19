package config

import (
	"bytes"
	"path/filepath"
	"strings"
	"text/template"

	toml "github.com/pelletier/go-toml/v2"

	_ "embed"

	cmtos "github.com/ice-blockchain/cometbft/internal/os"
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

	if err := toml.Unmarshal(confBytes, &conf); err != nil {
		panic(err)
	}
	return &conf
}

// Note: any changes to the comments/variables/mapstructure
// must be reflected in the appropriate struct in config/config.go.
//
//go:embed config.toml.tpl
var defaultConfigTemplate string
