// Config
package commands

import (
	"CollaborationNode/utils"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

func init() {

	utils.SetupConfigCommand(cmdConfig)
}

var cmdConfig = &cobra.Command{
	Use: "config",
	Short: `This command allows you to acces and modify the permanent configuration 
			of the ocp node. A node restart is required for changes to take effect`,

	Run: func(cmd *cobra.Command, args []string) {

		conf := utils.GetConfigMap()

		if conf != nil {
			b, _ := json.MarshalIndent(conf, "", "  ")
			fmt.Printf("Config File: %s\n\n", utils.ConfigDir.Path)
			fmt.Println(string(b))
			return
		}
		fmt.Println("Empty configuration")
	},
}
