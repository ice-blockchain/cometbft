package main

import (
	"os"
	"path/filepath"

	cmd "github.com/ice-blockchain/cometbft/cmd/multiplex/commands"
	cfg "github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/cli"
	mx "github.com/ice-blockchain/cometbft/multiplex"
)

func main() {
	rootCmd := cmd.RootCmd
	rootCmd.AddCommand(
		cmd.InitMxFilesCmd,
		cmd.GenUsersCmd,
		cmd.GenSeedsCmd,
		cli.NewCompletionCmd(rootCmd, true),
	)

	// NOTE:
	// Users wishing to:
	//	* Use an external signer for their validators
	//	* Supply an in-proc abci app
	//	* Supply a genesis doc file from another source
	//	* Provide their own DB implementation
	// can copy this file and use something other than the
	// DefaultNewNodesMultiplex function
	mxNodeFunc := mx.DefaultNewNodesMultiplex
	rootCmd.AddCommand(cmd.NewRunMultiplexCmd(mxNodeFunc))

	cmd := cli.PrepareBaseCmd(rootCmd, "CMT", os.ExpandEnv(filepath.Join("$HOME", cfg.DefaultCometDir)))
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
