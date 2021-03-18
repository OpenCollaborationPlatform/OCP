// p2p.go
package commands

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/OpenCollaborationPlatform/OCP /node"
	"github.com/OpenCollaborationPlatform/OCP /utils"

	"github.com/libp2p/go-libp2p-crypto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

//flag variables
var (
	force  bool
	detach bool
)

func init() {

	//add flags
	cmdInit.Flags().BoolVarP(&force, "force", "f", false, "Reinitialize even if already initialized or used otherwise")
	cmdStart.Flags().BoolVarP(&detach, "detach", "d", false, "Detach process and return from call")

	utils.AddConfigFlag(cmdStart, "api.port")
	utils.AddConfigFlag(cmdStart, "api.uri")
	utils.AddConfigFlag(cmdStart, "p2p.uri")
	utils.AddConfigFlag(cmdStart, "p2p.port")
	utils.AddConfigFlag(cmdStart, "log.json")
	utils.AddConfigFlag(cmdStart, "log.level")
	utils.AddConfigFlag(cmdStart, "log.file.enable")
}

var cmdVersion = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of the ocp node",
	Long: `The version of the ocp node. It prints the version of the called one 
	        	no the running one. Call just \'ocp\' to see verison of that.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(node.OcpVersion)
	},
}

var cmdStart = &cobra.Command{
	Use:   "start",
	Short: "Starts up the ocp node",
	Long: `The node will be started up if there is no node running yet. Otherwise an 
			error will be printed. Note: This command blocks till node is closed`,

	Run: func(cmd *cobra.Command, args []string) {

		if isConnected {
			fmt.Println("OCP node is already running. There cannot be a second node.")
			return
		}

		if detach {

			//we simply run a new process without detach argument,
			//which will than have a system parent

			//filter the -d out of the args: everything else we want to keep
			args := make([]string, 0)
			for _, arg := range os.Args {
				if arg != "-d" && arg != "--detach" {
					args = append(args, arg)
				}
			}
			cmd := exec.Command(args[0], args[1:]...)
			err := cmd.Start()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1) //exit with failure
			}

		} else {

			//start the node
			ocpNode = node.NewNode()
			err := ocpNode.Start()
			if err != nil {
				os.Exit(1) //exit with failure
			}

			//setup all online commands
			initOnlineCommands()

			//wait till someone wants to stop...
			ocpNode.WaitForStop()
		}
	},
}

var cmdStop = &cobra.Command{
	Use:   "stop",
	Short: "Stops the running ocp node",
	Long: `The node will be stoped if there is one running. Otherwise an 
			error will be printed`,

	Run: onlineCommand("stop", func(ctx context.Context, args []string, flags map[string]interface{}) string {
		defer func() { ocpNode.Stop(ctx, "Shutdown request received") }()
		return ""
	}),
}

var cmdInit = &cobra.Command{
	Use:   "init [directory]",
	Short: "Initializes a directory to be used as node storage, optional at user defined path",
	Long: `OCP node needs a directory to store runtime as well as project data. With 
			 this command this directory is prepared. It is either a default directory or a user supplied one`,
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

		//user may have specified something special
		nodeDir := utils.GetDefaultNodeFolder()
		if len(args) == 1 {
			nodeDir = args[0]
		}

		//see if we have a folder already
		currentDir := viper.GetString("directory")
		_, err := os.Stat(currentDir)
		if !os.IsNotExist(err) {
			if nodeDir == currentDir && !force {
				fmt.Println("Specified directiory already initialized. Nothing done.")
				return
			}
		}

		//setup the folder
		_, err = os.Stat(nodeDir)
		if os.IsNotExist(err) {

			if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
				fmt.Printf("Error creating the node folder \"%s\": %s\n", nodeDir, err)
				return
			}
		} else {

			if !force {
				fmt.Printf("Folder already in use. Specify --force to reuse it")
				return
			}
			//make it free for use to use
			if err := os.RemoveAll(nodeDir); err != nil {
				fmt.Printf("Error cleaning the node folder \"%s\": %s", nodeDir, err)
				return
			}
			if err := os.MkdirAll(nodeDir, os.ModePerm); err != nil {
				fmt.Printf("Error cleaning the node folder \"%s\": %s\n", nodeDir, err)
				return
			}
		}

		viper.Set("directory", nodeDir)
		err = viper.WriteConfig()
		if err != nil {
			fmt.Printf("Could not store the directory information in config: %s\n", err)
			return
		}

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

		fmt.Printf("Node directory was initialized: %s\n", nodeDir)

	},
}
