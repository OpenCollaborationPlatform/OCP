// Config
package commands

import (
	"context"
	"fmt"
	"github.com/ickby/CollaborationNode/utils"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

//flag variables
var (
	online         bool
	onlineReadConf func(*cobra.Command, []string)
)

func readConf(args []string) string {

	result := ""

	var keys []string
	if len(args) == 0 {
		result = fmt.Sprintf("Config file: %v\n", viper.ConfigFileUsed())
		keys = viper.AllKeys()

	} else {
		//first check if it is a valid accessor
		if !viper.IsSet(args[0]) {
			result = fmt.Sprintf("Not a valid config entry")
			return result
		}

		entry := viper.Get(args[0])
		switch entry.(type) {
		case map[string]interface{}:
			keys = viper.Sub(args[0]).AllKeys()
		default:
			result = fmt.Sprintf("%v", entry)
			return result
		}
	}

	var groups []string
	sort.Strings(keys)
	for _, key := range keys {

		parts := strings.Split(key, ".")

		//get the first entry that is different than our current group
		firstChanged := len(groups)
		for i, v := range groups {
			if v != parts[i] {
				firstChanged = i
				break
			}
		}

		//groups got a newine for better visual separation, do the same for ungrouped entries
		if len(parts) == 1 && len(groups) >= 1 {
			result += fmt.Sprintf("\n")
		}

		//write the groups
		groups = groups[:firstChanged]
		for i := firstChanged; i < len(parts)-1; i++ {
			groups = append(groups, parts[i])
			indent := strings.Repeat("   ", i)
			result += fmt.Sprintf("\n%s%s\n", indent, parts[i])
		}

		//write the value
		indent := strings.Repeat("   ", len(parts)-1)
		result += fmt.Sprintf("%s%s: %v", indent, parts[len(parts)-1], viper.Get(key))

		conf, err := utils.GetConfigEntry(key)
		if err == nil {
			default_ := conf.Default
			if default_ != nil && fmt.Sprintf("%v", viper.Get(key)) != fmt.Sprintf("%v", default_) {
				result += fmt.Sprintf(" (default: %v)", default_)
			}
		}
		result += fmt.Sprintf("\n")
	}

	return result
}

func init() {

	//to be able to get the config of the running node we need to register a
	//online command
	onlineReadConf = onlineCommand("config", func(ctx context.Context, args []string, flags map[string]interface{}) string {
		return readConf(args)
	})

	cmdConfig.AddCommand(cmdConfigWrite, cmdConfigCreate, cmdConfigRemove)
	cmdConfig.Flags().BoolVarP(&online, "online", "o", false, "Check the config of the running node, which could be different due to use of command line flags")
}

var cmdConfig = &cobra.Command{
	Use:   "config [subconf]",
	Short: `Create, access and modify the node configuration`,
	Long:  `By default prints the whole currently selected configuration. For subconfigurations provide the chain of keys`,
	Args:  cobra.MaximumNArgs(1),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setup(false)
	},

	Run: func(cmd *cobra.Command, args []string) {

		if online {
			onlineReadConf(cmd, args)

		} else {
			println(readConf(args))
		}
	},
}

var cmdConfigWrite = &cobra.Command{
	Use:   "write",
	Short: "write [accessor] [value] Writes value to the current selected config",
	Long:  "Writes a config value to the config file. The setting will not be used by a already running node",
	Args:  cobra.ExactArgs(2),

	Run: func(cmd *cobra.Command, args []string) {

		//first check if it is a valid accessor
		if !viper.IsSet(args[0]) {
			fmt.Println("Not a valid config entry")
			return
		}

		entry, err := utils.GetConfigEntry(args[0])
		if err != nil {
			fmt.Printf(err.Error())
			return
		}

		val, err := entry.ValueFromString(args[1])
		if err != nil {
			fmt.Printf(err.Error())
			return
		}

		viper.Set(args[0], val)
		err = viper.WriteConfig()
		if err != nil {
			fmt.Printf(err.Error())
		}
		fmt.Println("Configuration updated")
	},
}

var cmdConfigCreate = &cobra.Command{
	Use:   "create",
	Short: "create [name] Creates new config file with given name",
	Long: `Creates a named config file which can be used as alternativ to the default one. It will be stored in the
			default config folder. The create file can be used via the global --config flag, e.g\n
			ocp new MyConfigFile \nocp start --config MyConfigFile`,
	Args: cobra.ExactArgs(1),

	Run: func(cmd *cobra.Command, args []string) {

		name, err := utils.CreateConfigFile(args[0])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("Successfully created config file. Use it with --config %s\n", name)
	},
}

var cmdConfigRemove = &cobra.Command{
	Use:   "remove",
	Short: "remove [name] Removes config file with given name",
	Args:  cobra.ExactArgs(1),

	Run: func(cmd *cobra.Command, args []string) {

		err := utils.RemoveConfigFile(args[0])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("Successfully removed config file\n")
	},
}
