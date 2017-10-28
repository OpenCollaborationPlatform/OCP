// Commands
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/jcelliott/turnpike.v2"
)

var nodeClient *turnpike.Client = nil
var isConnected bool = false

//flag variables
var (
	//init
	createNodeDir bool
	forceCreation bool
	nodeDir       string

	//start
	port int
)

func getCommands() *cobra.Command {

	//add flags
	cmdInit.Flags().BoolVarP(&createNodeDir, "create", "c", false, "Create directory if it does not exist")
	cmdInit.Flags().BoolVarP(&forceCreation, "force", "f", false, "Creates the directory even if there already exists one")
	cmdInit.Flags().StringVarP(&nodeDir, "path", "p", "", "Specify alternative folder to be used")
	cmdStart.Flags().IntVarP(&port, "port", "p", 8000, "Specify the port on which the node listens for clients")

	rootCmd.AddCommand(cmdVersion, cmdStart, cmdStop, cmdInit, cmdConfig)
	return rootCmd
}

var rootCmd = &cobra.Command{
	Use:   "ocp",
	Short: "OCP is the open collaboration platform node",
	Long: `A node within the open collaboration platform network which provides 
		 	 access to all functionality of the eco system and handles the datastructures`,

	PersistentPreRun: func(cmd *cobra.Command, args []string) {

		//try to get the client to our running node
		pid, port, err := ReadPidPort()
		if err != nil {
			panic(fmt.Sprintf("Problem with pid file: %s", err))
		}

		if pid == -1 { //definitely not connected
			return
		}

		c, err := turnpike.NewWebsocketClient(turnpike.JSON, fmt.Sprintf("ws://localhost:%v/", port), nil, nil, nil)
		if err != nil { //cannot connect means PID is wrong or process hangs
			ClearPidPort()
			return
		}
		_, err = c.JoinRealm("ocp", nil)
		if err != nil { //seems like a wrong wamp server...
			ClearPidPort()
			return
		}
		isConnected = true
		nodeClient = c
	},

	Run: func(cmd *cobra.Command, args []string) {

		//check if a node is running and return info about it
		if !isConnected {
			fmt.Println("No node is currently running.")
			return
		}

		fmt.Println("Currently connected to a ocp node")
	},
}

var cmdVersion = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of the ocp node",
	Long: `The version of the ocp node. It prints the version of the called one 
	        	no the running one`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("v0.1 development version")
	},
}

var cmdStart = &cobra.Command{
	Use:   "start",
	Short: "Starts up the ocp node",
	Long: `The node will be started up if there is no node running yet. Otherwise an 
			error will be printed`,

	Run: func(cmd *cobra.Command, args []string) {
		startup()
	},
}

var cmdStop = &cobra.Command{
	Use:   "stop",
	Short: "Stops the running ocp node",
	Long: `The node will be sttoped if there is one running. Otherwise an 
			error will be printed`,

	Run: func(cmd *cobra.Command, args []string) {
		//check if a node is running and return info about it
		if !isConnected {
			fmt.Println("No node is currently running.")
			return
		}

		if _, err := nodeClient.Call("ocp.command.stop", nil, nil, nil); err != nil {
			fmt.Println("Error shutting down:", err)
		}
	},
}

var cmdInit = &cobra.Command{
	Use:   "init",
	Short: "Initializes a directory to be used as node storage",
	Long: `OCP node needs a directory to store runtime as well as project data. With 
			 this command this directory is prepared`,
	Args: cobra.MaximumNArgs(1),

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 1 {
			fmt.Println("Initialization failed: to many arguments")
			return
		}

		if nodeDir == "" {
			nodeDir = getDefaultNodeFolder()
		}

		_, err := os.Stat(nodeDir)
		if os.IsNotExist(err) {

			if createNodeDir {
				if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
					fmt.Printf("Error creating the node folder \"%s\": %s", nodeDir, err)
					return
				}
			} else {
				fmt.Println("Folder does not exist. Please create it or use -c flag")
				return
			}
		}

		saveToConfigV(nodeDir, "directory")
	},
}
