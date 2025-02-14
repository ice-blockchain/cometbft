package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/cli"
	cmtflags "github.com/ice-blockchain/cometbft/libs/cli/flags"
	"github.com/ice-blockchain/cometbft/libs/log"

	cmtcmd "github.com/ice-blockchain/cometbft/cmd/cometbft/commands"
)

var (
	nodeConfig = config.DefaultConfig()
	nodeLogger = log.NewTMLogger(log.NewSyncWriter(os.Stdout))
)

func init() {
	registerFlagsRootCmd(RootCmd)
}

func registerFlagsRootCmd(cmd *cobra.Command) {
	cmd.PersistentFlags().String("log_level", nodeConfig.LogLevel, "log level")
}

// ParseConfig retrieves the default environment configuration,
// sets up the CometBFT root and ensures that the root exists.
func ParseConfig(cmd *cobra.Command) (*config.Config, error) {
	conf := config.DefaultConfig()
	err := viper.Unmarshal(conf)
	if err != nil {
		return nil, err
	}

	var home string
	switch {
	case os.Getenv("CMTHOME") != "":
		home = os.Getenv("CMTHOME")
	case os.Getenv("TMHOME") != "":
		// XXX: Deprecated.
		home = os.Getenv("TMHOME")
		nodeLogger.Error("Deprecated environment variable TMHOME identified. CMTHOME should be used instead.")
	default:
		home, err = cmd.Flags().GetString(cli.HomeFlag)
		if err != nil {
			return nil, err
		}
	}

	conf.RootDir = home

	conf.SetRoot(conf.RootDir)
	if err := config.EnsureFilesystem(conf.RootDir); err != nil {
		return nil, fmt.Errorf("error in filesystem: %w", err)
	}
	if err := conf.ValidateBasic(); err != nil {
		return nil, fmt.Errorf("error in config file: %v", err)
	}
	if warnings := conf.CheckDeprecated(); len(warnings) > 0 {
		for _, warning := range warnings {
			nodeLogger.Info("deprecated usage found in configuration file", "usage", warning)
		}
	}
	return conf, nil
}

// RootCmd is the root command for CometBFT core.
var RootCmd = &cobra.Command{
	Use:   "cometbft",
	Short: "BFT state machine replication for applications in any programming languages",
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) (err error) {
		if cmd.Name() == cmtcmd.VersionCmd.Name() {
			return nil
		}

		nodeConfig, err = ParseConfig(cmd)
		if err != nil {
			return err
		}

		for _, possibleMisconfiguration := range nodeConfig.PossibleMisconfigurations() {
			nodeLogger.Info(possibleMisconfiguration)
		}

		if nodeConfig.LogFormat == config.LogFormatJSON {
			nodeLogger = log.NewTMJSONLogger(log.NewSyncWriter(os.Stdout))
		}

		nodeLogger, err = cmtflags.ParseLogLevel(nodeConfig.LogLevel, nodeLogger, config.DefaultLogLevel)
		if err != nil {
			return err
		}

		if viper.GetBool(cli.TraceFlag) {
			nodeLogger = log.NewTracingLogger(nodeLogger)
		}

		nodeLogger = nodeLogger.With("module", "main")
		return nil
	},
}
