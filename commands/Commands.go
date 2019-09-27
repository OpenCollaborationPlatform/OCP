// Commands
package commands

import (
	"time"
	"github.com/ickby/CollaborationNode/node"
	"github.com/ickby/CollaborationNode/utils"
	"context"
	"fmt"
	"log"

	nxclient "github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	golog "github.com/ipfs/go-log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	gologging "github.com/whyrusleeping/go-logging"
)

var nodeClient *nxclient.Client = nil
var isConnected bool = false

var (
	onlineCMDs []func(*node.Node) //all functions needed to setup the online commands
	ocpNode    *node.Node
	configPath string
	verbose    bool
)

func Execute() {

	//flags
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "default", "Set configfile to use instead of system config")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable extra output like debug messages")

	rootCmd.AddCommand(cmdVersion, cmdStart, cmdStop, cmdInit, cmdConfig)
	rootCmd.Execute()
}

func initOnlineCommands() {

	if ocpNode == nil {
		log.Fatal("OCP node is NIL, aborting")
	}

	for _, f := range onlineCMDs {
		f(ocpNode)
	}
}

//this is a herlper function which setups a function to be accessible via the router
//so that it can be called by normal cobra command
func onlineCommand(name string, f func(context.Context, []string, map[string]interface{}) string) func(*cobra.Command, []string) {

	onlineCMDs = append(onlineCMDs, func(node *node.Node) {

		//register the function to be callable via WAMP
		cmdClient, err := node.Router.GetLocalClient("command")
		if err != nil {
			log.Fatalf("Unable to setup command client: %s", err)
		}

		//make a wrapper function that is WAMP callable
		wrapper := func(ctx context.Context, wampargs wamp.List, wampkwargs wamp.Dict, wampdetails wamp.Dict) *nxclient.InvokeResult {

			//a string argument list
			slice := make([]string, len(wampargs))
			for i, value := range wampargs {
				slice[i] = value.(string)
			}

			//call the function
			result := f(ctx, slice, wampkwargs)

			//postprocess the result
			return &nxclient.InvokeResult{Args: wamp.List{result}}
		}

		//register it to be callable
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

		//build the flag list
		var flags = make(wamp.Dict, 0)
		cmd.Flags().VisitAll(func(flag *pflag.Flag) {

			switch flag.Value.Type() {
			case "string":
				val, _ := cmd.Flags().GetString(flag.Name)
				flags[flag.Name] = val
			case "bool":
				val, _ := cmd.Flags().GetBool(flag.Name)
				flags[flag.Name] = val
			case "int":
				val, _ := cmd.Flags().GetInt(flag.Name)
				flags[flag.Name] = val
			default:
				log.Fatalf("Unsupported flag type, please implement: %s", flag.Value.Type())
			}
		})

		//build the wamp argument list
		slice := make(wamp.List, len(args))
		for i, value := range args {
			slice[i] = value
		}

		//call the node command
		ctx,_ := context.WithTimeout(context.Background(), 5*time.Second)
		result, err := nodeClient.Call(ctx, fmt.Sprintf("ocp.command.%s", name), nil, slice, flags, "")
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

	//config from flag
	utils.InitConfig(configPath)

	//output from flag
	if verbose {
		golog.SetAllLoggers(gologging.DEBUG)
	}

	//try to get the client to our running node
	pid, port, err := utils.ReadPidPort()
	if err != nil && pidPortPanic {
		log.Fatalf("Problem with pid file: %s", err)
	}

	if pid == -1 { //definitely not connected
		return
	}

	//authFunc := func(c *wamp.Challenge) (string, wamp.Dict) {
	//	return "", wamp.Dict{}
	//}
	cfg := nxclient.ClientConfig{
		Realm:        "ocp",
		HelloDetails: wamp.Dict{"authid": "command", "role": "local"},
		//AuthHandlers: map[string]nxclient.AuthFunc{"ticket": authFunc},
	}
	c, err := nxclient.ConnectNet(fmt.Sprintf("ws://localhost:%v/", port), cfg)

	if err != nil { //cannot connect means PID is wrong or process hangs
		err := utils.ClearPidPort()
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

	Run: onlineCommand("ocp", func(ctx context.Context, args []string, flags map[string]interface{}) string {

		s := "OCP node running\n"
		s += fmt.Sprintf("Version: 	%s\n", ocpNode.Version)
		s += fmt.Sprintf("ID: 		%s\n", ocpNode.Host.ID().Pretty())
		return s
	}),
}
