// Copyright © 2017 Naveego
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/naveego/navigator-go/subscribers/server"
	"github.com/naveego/pipeline-subscribers/shapeutils"
	"github.com/spf13/cobra"
)

var verbose *bool

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "mariadb [listen address]",
	Args:  cobra.ExactArgs(1),
	Short: "A subscriber that sends all data to MariaDB",
	Long: `Settings should contain a DataSourceName property with a value 
corresponding to the standard MariaDB/MySQL connection string: "user:password@address:port/database".

User must have CREATE and ALTER permissions.`,

	Run: func(cmd *cobra.Command, args []string) {

		logrus.SetOutput(os.Stdout)

		addr := args[0]

		if *verbose {
			logrus.SetLevel(logrus.DebugLevel)
		}

		subscriber := &mariaSubscriber{
			knownShapes: shapeutils.NewShapeCache(),
		}

		srv := server.NewSubscriberServer(addr, subscriber)

		go func() {
			err := srv.ListenAndServe()
			if err != nil {
				panic(err)
			}
		}()

		<-awaitShutdown()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	verbose = flag.Bool("v", false, "enable verbose logging")
}

func awaitShutdown() <-chan bool {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	// `signal.Notify` registers the given channel to
	// receive notifications of the specified signals.
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// This goroutine executes a blocking receive for
	// signals. When it gets one it'll print it out
	// and then notify the program that it can finish.
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	fmt.Println("CTRL-C to quit")

	return done
}
