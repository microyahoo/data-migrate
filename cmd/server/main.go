package main

import (
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/microyahoo/data-migrate/pkg/server"
)

var (
	configFile   string
	serverPort   int
	debug, trace bool
)

func main() {
	rootCmd := newCommand()
	cobra.CheckErr(rootCmd.Execute())
}

func newCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use: "data-capacity-statistics-server",
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
	cmds.Flags().SortFlags = false

	viper.SetDefault("DEBUG", false)
	viper.SetDefault("TRACE", false)
	viper.SetDefault("SERVERPORT", 2000)

	viper.AutomaticEnv()
	viper.AllowEmptyEnv(true)

	cmds.Flags().StringVar(&configFile, "config.file", viper.GetString("CONFIGFILE"), "Config file describing test run")
	cmds.Flags().BoolVar(&debug, "debug", viper.GetBool("DEBUG"), "enable debug log output")
	cmds.Flags().BoolVar(&trace, "trace", viper.GetBool("TRACE"), "enable trace log output")
	cmds.Flags().IntVar(&serverPort, "server.port", viper.GetInt("SERVERPORT"), "Port on which the server will be available for clients. Default: 2000")

	// cmds.MarkFlagRequired("config.file")

	return cmds
}

func run() {
	if configFile == "" {
		log.Fatal("--config.file is a mandatory parameter - please specify the config file")
	}
	if debug {
		log.SetLevel(log.DebugLevel)
	} else if trace {
		log.SetLevel(log.TraceLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.Debugf("viper settings: %+v", viper.AllSettings())
	log.Debugf("data-capacity-statistics server configFile: %s, serverPort: %d", configFile, serverPort)

	server, err := server.NewServer(configFile)
	if err != nil {
		log.Fatalf("failed to create server: %s", err)
	}
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			completed, total, success, failed, clients := server.GetProgress()
			log.Infof("Progress: %d/%d tasks completed(succcess: %d, failed: %d), %d clients: %s", completed, total, success, failed, len(clients), clients)
		}
	}()
	server.Start(serverPort)
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	rand.New(rand.NewSource(time.Now().UnixNano()))
}
