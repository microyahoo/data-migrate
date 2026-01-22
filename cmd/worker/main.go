package main

import (
	"math/rand"
	"os/exec"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/microyahoo/data-migrate/pkg/worker"
)

var (
	debug, trace  bool
	serverAddress string
	concurrency   int
)

func main() {
	rootCmd := newCommand()
	cobra.CheckErr(rootCmd.Execute())
}

func newCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use: "data-capacity-statistics-worker",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			_, err := exec.Command("rclone", "version").CombinedOutput()
			if err != nil {
				return err
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
	cmds.Flags().SortFlags = false

	viper.SetDefault("DEBUG", false)
	viper.SetDefault("TRACE", false)
	viper.SetDefault("PROMETHEUSPORT", 8888)

	viper.AutomaticEnv()
	viper.AllowEmptyEnv(true)

	cmds.Flags().BoolVar(&trace, "trace", viper.GetBool("TRACE"), "enable trace log output")
	cmds.Flags().BoolVar(&debug, "debug", viper.GetBool("DEBUG"), "enable debug log output")
	cmds.Flags().StringVar(&serverAddress, "server.address", viper.GetString("SERVERADDRESS"), "Data-migrate Server IP and Port in the form '191.168.1.1:2000'")
	cmds.Flags().IntVar(&concurrency, "concurrency", viper.GetInt("CONCURRENCY"), "Concurrency for rclone tasks, default 3")

	// cmds.MarkFlagRequired("server.address")

	return cmds
}

func run() {
	if serverAddress == "" {
		log.Fatal("--server.address is a mandatory parameter - please specify the server IP and Port")
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	} else if trace {
		log.SetLevel(log.TraceLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.Debugf("viper settings: %+v", viper.AllSettings())
	log.Debugf("data-capacity-statistics worker serverAddress: %s", serverAddress)

	// TODO: functional parameters
	worker.NewWorker(serverAddress, concurrency).Start()
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	rand.New(rand.NewSource(time.Now().UnixNano()))
}
