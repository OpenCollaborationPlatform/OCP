// Config
package commands

import (
	"CollaborationNode/utils"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {

	cmdConfig.AddCommand(cmdConfigWrite, cmdConfigCreate, cmdConfigRemove)
}

var cmdConfig = &cobra.Command{
	Use: "config",
	Short: `This command allows you to acces and modify the permanent configuration 
			of the ocp node. A node restart is required for changes to take effect`,
	Args: cobra.MaximumNArgs(1),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setup(false)
	},

	Run: func(cmd *cobra.Command, args []string) {

		var keys []string
		if len(args) == 0 {
			keys = viper.AllKeys()

		} else {
			//first check if it is a valid accessor
			if !viper.IsSet(args[0]) {
				fmt.Println("Not a valid config entry")
				return
			}

			entry := viper.Get(args[0])
			switch entry.(type) {
			case map[string]interface{}:
				keys = viper.Sub(args[0]).AllKeys()
			default:
				fmt.Printf("%v\n", entry)
				return
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
				fmt.Println("")
			}

			//write the groups
			groups = groups[:firstChanged]
			for i := firstChanged; i < len(parts)-1; i++ {
				groups = append(groups, parts[i])
				indent := strings.Repeat("   ", i)
				fmt.Printf("\n%s%s\n", indent, parts[i])
			}

			//write the value
			indent := strings.Repeat("   ", len(parts)-1)
			fmt.Printf("%s%s: %v", indent, parts[len(parts)-1], viper.Get(key))

			default_ := utils.GetConfigEntry(key).Default
			if default_ != nil && fmt.Sprintf("%v", viper.Get(key)) != fmt.Sprintf("%v", default_) {
				fmt.Printf(" (default: %v)", default_)
			}
			fmt.Println("")
		}
	},
}

var cmdConfigWrite = &cobra.Command{
	Use:   "write",
	Short: "write [accessor] [value] Writes value for given config to the file",
	Long:  "Writes a config value to the config file. The setting will not be used by a already running node",
	Args:  cobra.ExactArgs(2),

	Run: func(cmd *cobra.Command, args []string) {

		//first check if it is a valid accessor
		if !viper.IsSet(args[0]) {
			fmt.Println("Not a valid config entry")
			return
		}

		utils.SaveToConfigV(args[1], args[0])
	},
}

var cmdConfigCreate = &cobra.Command{
	Use:   "create",
	Short: "create [name] Creates new config file with given name",
	Long: "Creates a named config file which can be used as alternativ to the default one. It will be stored in the " +
		"default config folder. The create file can be used via the global --config flag, e.g\n" +
		"ocp new MyConfigFile \nocp start --config MyConfigFile",
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
		fmt.Printf("Successfully removed config file")
	},
}
