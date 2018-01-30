// p2p.go
package commands

import (
	"CollaborationNode/node"
	"CollaborationNode/utils"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p-crypto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

//flag variables
var (
	createNodeDir bool
	forceCreation bool
	nodeDir       string
)

func init() {

	//add flags
	cmdInit.Flags().BoolVarP(&createNodeDir, "create", "c", false, "Create directory if it does not exist")
	cmdInit.Flags().BoolVarP(&forceCreation, "force", "f", false, "Creates the directory even if there already exists one")
	cmdInit.Flags().StringVarP(&nodeDir, "path", "p", "", "Specify alternative folder to be used")

	utils.AddConfigFlag(cmdStart, "connection.port")
}

var cmdVersion = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of the ocp node",
	Long: `The version of the ocp node. It prints the version of the called one 
	        	no the running one. Call just \'ocp\' to see verison of that.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(ocpNode.Version)
	},
}

var cmdStart = &cobra.Command{
	Use:   "start",
	Short: "Starts up the ocp node",
	Long: `The node will be started up if there is no node running yet. Otherwise an 
			error will be printed`,

	Run: func(cmd *cobra.Command, args []string) {

		if isConnected {
			fmt.Println("OCP node is already running. There cannot be a second node.")
			return
		}

		//start the node
		ocpNode = node.NewNode()
		ocpNode.Start()

		//setup all online commands
		initOnlineCommands()

		//wait till someone wants to stop...
		ocpNode.WaitForStop()
	},
}

var cmdStop = &cobra.Command{
	Use:   "stop",
	Short: "Stops the running ocp node",
	Long: `The node will be sttoped if there is one running. Otherwise an 
			error will be printed`,

	Run: onlineCommand("stop", func([]string) string {
		defer func() { ocpNode.Stop("Shutdown request received") }()
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
			nodeDir = utils.GetDefaultNodeFolder()
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

		utils.SaveToConfigV(nodeDir, "directory")
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
