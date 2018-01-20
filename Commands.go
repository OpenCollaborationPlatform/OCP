// Commands
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/libp2p/go-libp2p-crypto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var nodeClient *nxclient.Client = nil
var isConnected bool = false

//flag variables
var (
	//init
	createNodeDir bool
	forceCreation bool
	nodeDir       string
	onlineCMDs    []func()
)

func getCommands() *cobra.Command {

	//add flags
	cmdInit.Flags().BoolVarP(&createNodeDir, "create", "c", false, "Create directory if it does not exist")
	cmdInit.Flags().BoolVarP(&forceCreation, "force", "f", false, "Creates the directory even if there already exists one")
	cmdInit.Flags().StringVarP(&nodeDir, "path", "p", "", "Specify alternative folder to be used")

	addFlag(cmdStart, "connection.port")

	rootCmd.AddCommand(cmdVersion, cmdStart, cmdStop, cmdInit, cmdConfig)
	return rootCmd
}

func initOnlineCommands() {

	for _, f := range onlineCMDs {
		f()
	}
}

//this is a herlper function which setups a function to be accessible via the router
//so that it can be called by normal cobra command
func onlineCommand(name string, f func([]string) string) func(*cobra.Command, []string) {

	onlineCMDs = append(onlineCMDs, func() {

		//register the function to be callable via WAMP
		cmdClient, err := router.GetLocalClient("command")
		if err != nil {
			log.Fatalf("Unable to setup command client: %s", err)
		}

		//make a wrapper function that is WAMP callable
		wrapper := func(ctx context.Context, wampargs wamp.List, wampkwargs, wampdetails wamp.Dict) *nxclient.InvokeResult {

			//a string argument list
			slice := make([]string, len(wampargs))
			for i, value := range wampargs {
				slice[i] = value.(string)
			}

			//call the function
			result := f(slice)

			//postprocess the result
			return &nxclient.InvokeResult{Args: wamp.List{result}}
		}

		//register it to be callable
		log.Println("registerd command")
		if err := cmdClient.Register(fmt.Sprintf("ocp.command.%s", name), wrapper, nil); err != nil {
			log.Fatalf("Registry error: %s", err)
		}
	})

	//build the cobra command function that calls our just registered one
	cmdFunc := func(cmd *cobra.Command, args []string) {

		//check if a node is running and return info about it
		if !isConnected {
			fmt.Println("No node is currently running. Aborting.")
			return
		}

		//build the wamp argument list
		slice := make(wamp.List, len(args))
		for i, value := range args {
			slice[i] = value
		}

		//call the node command
		ctx := context.Background()
		result, err := nodeClient.Call(ctx, fmt.Sprintf("ocp.command.%s", name), nil, slice, nil, "")
		if err != nil {
			fmt.Println("Error:", err)
		}

		//postprocess the result and print it
		str := result.Arguments[0].(string)
		if str != "" {
			fmt.Println(str)
		}
	}

	return cmdFunc
}

func setup(pidPortPanic bool) {

	//try to get the client to our running node
	pid, port, err := ReadPidPort()
	if err != nil && pidPortPanic {
		log.Fatalf("Problem with pid file: %s", err)
	}

	if pid == -1 { //definitely not connected
		return
	}

	authFunc := func(c *wamp.Challenge) (string, wamp.Dict) {
		return "", wamp.Dict{}
	}
	cfg := nxclient.ClientConfig{
		Realm:        "ocp",
		HelloDetails: wamp.Dict{"authid": "command", "role": "local"},
		AuthHandlers: map[string]nxclient.AuthFunc{"ticket": authFunc},
	}
	c, err := nxclient.ConnectNet(fmt.Sprintf("ws://localhost:%v/", port), cfg)

	if err != nil { //cannot connect means PID is wrong or process hangs
		err := ClearPidPort()
		if err != nil && pidPortPanic {
			log.Fatalf("Problem with pid file: %s", err)
		}
		return
	}

	isConnected = true
	nodeClient = c
}

var rootCmd = &cobra.Command{
	Use:   "ocp",
	Short: "OCP is the open collaboration platform node",
	Long: `A node within the open collaboration platform network which provides 
		 	 access to all functionality of the eco system and handles the datastructures`,

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setup(true)
	},

	Run: func(cmd *cobra.Command, args []string) {

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

	Run: onlineCommand("stop", func([]string) string {
		defer func() { quit <- "Shutdown request received" }()
		return ""
	}),
}

var cmdInit = &cobra.Command{
	Use:   "init",
	Short: "Initializes a directory to be used as node storage",
	Long: `OCP node needs a directory to store runtime as well as project data. With 
			 this command this directory is prepared`,
	Args: cobra.MaximumNArgs(1),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		//we override root persistant prerun to not panic out on no init
		setup(false)
	},
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 1 {
			fmt.Println("Initialization failed: to many arguments")
			return
		}

		if nodeDir == "" {
			nodeDir = getDefaultNodeFolder()
		}

		//see if we have a folder already
		currentDir := viper.GetString("directory")
		_, err := os.Stat(currentDir)
		if err == nil {
			if nodeDir == currentDir {
				fmt.Println("Specified directiory already initialized. Nothing done.")
				return
			}
			if !forceCreation {
				fmt.Printf("There is a initialized directiory already, use --force to override: %s\n", currentDir)
				return
			}
		}

		//setup the folder
		_, err = os.Stat(nodeDir)
		if os.IsNotExist(err) {

			if createNodeDir {
				if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
					fmt.Printf("Error creating the node folder \"%s\": %s\n", nodeDir, err)
					return
				}
			} else {
				fmt.Println("Folder does not exist. Please create it or use -c flag")
				return
			}
		}

		saveToConfigV(nodeDir, "directory")
		fmt.Printf("Node directory was initialized: %s\n", nodeDir)

		//Generate our node keys (and hence identity)
		priv, pub, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
		if err != nil {
			fmt.Printf("Could not create key pair: %s\n", err)
			return
		}

		bytes, _ := crypto.MarshalPublicKey(pub)
		err = ioutil.WriteFile(filepath.Join(nodeDir, "public"), bytes, 0644)
		if err != nil {
			fmt.Printf("Could not create public key file: %s\n", err)
			return
		}

		bytes, _ = crypto.MarshalPrivateKey(priv)
		err = ioutil.WriteFile(filepath.Join(nodeDir, "private"), bytes, 0644)
		if err != nil {
			fmt.Printf("Could not create private key file: %s\n", err)
			return
		}
	},
}

var cmdConfig = &cobra.Command{
	Use: "config",
	Short: `This command allows you to acces and modify the permanent configuration 
			of the ocp node. A node restart is required for changes to take effect`,

	Run: func(cmd *cobra.Command, args []string) {

		conf := getConfigMap()

		if conf != nil {
			b, _ := json.MarshalIndent(conf, "", "  ")
			fmt.Printf("Config File: %s\n\n", configDir.Path)
			fmt.Println(string(b))
			return
		}
		fmt.Println("Empty configuration")
	},
}
