// Config
package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/shibukawa/configdir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configDir *configdir.Config = nil

type ConfigEntry struct {
	Short   string
	Text    string
	Default interface{}
}

var (
	configEntries = map[string]interface{}{
		"connection": map[string]interface{}{
			"port": ConfigEntry{Default: 8000, Short: "p", Text: "The port on which the node listents for client connections"},
		},
	}

	writeValue string
	node       bool
)

func getConfigValue(keys ...string) ConfigEntry {

	//could be viper access string like connection.port
	if len(keys) == 1 {
		return getConfigValueByArray(strings.Split(keys[0], "."))
	}

	return getConfigValueByArray(keys)
}

func getConfigValueByArray(keys []string) ConfigEntry {

	//iterate over all nested values
	tmp := configEntries
	for i, key := range keys {
		if i == (len(keys) - 1) {
			return tmp[key].(ConfigEntry)
		}
		tmp = tmp[key].(map[string]interface{})
	}

	panic("No such config entry exists")
	return ConfigEntry{}
}

func initConfig() {

	configDirs := configdir.New("ocp", "")
	folders := configDirs.QueryFolders(configdir.Global)
	if len(folders) < 1 {
		fmt.Println("No folder for config found")
		return
	}

	configDir = folders[0]
	if !configDir.Exists("config.json") {
		configDir.Create("config.json")
		dummy := make(map[string]interface{})
		bytes, _ := json.Marshal(dummy)
		configDir.WriteFile("config.json", bytes)
	}

	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(configDir.Path)
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("While reading config file - \"%s\" \n", err))
	}

	setupConfigMap(configEntries, cmdConfig, "")
	setupConfigDefaults(configEntries, "")
}

var cmdConfig = &cobra.Command{
	Use: "config",
	Short: `This command allows you to acces and modify the permanent configuration 
			of the ocp node. A node restart is required for changes to take effect`,

	Run: func(cmd *cobra.Command, args []string) {

		conf := getConfigMap()

		if conf != nil {
			b, _ := json.MarshalIndent(conf, "", "  ")
			fmt.Println(string(b))
			return
		}
		fmt.Println("Empty configuration")
	},
}

func addFlag(cmd *cobra.Command, accessor string) {

	config := getConfigValue(accessor)
	keys := strings.Split(accessor, ".")
	name := keys[len(keys)-1]

	switch config.Default.(type) {
	case int:
		cmd.Flags().IntP(name, config.Short, viper.GetInt(accessor), config.Text)
	case string:
		cmd.Flags().StringP(name, config.Short, viper.GetString(accessor), config.Text)
	case float64:
		cmd.Flags().Float64P(name, config.Short, viper.GetFloat64(accessor), config.Text)
	default:
		panic(fmt.Sprintf("No flag can be created for config %s", accessor))
	}

	fmt.Printf("Bind flag: %s\n", name)
	viper.BindPFlag(accessor, cmd.Flags().Lookup(name))
}

func setupConfigMap(value map[string]interface{}, parent *cobra.Command, accessor string) {

	if parent == nil {
		panic("Parent command is NIL")
	}

	//build all subcommands
	for key, value := range value {

		var accessor_ string

		if accessor == "" {
			accessor_ = key
		} else {
			accessor_ = accessor + "." + key
		}

		//it is is a map we need to go on iterating
		if tmp, ok := value.(map[string]interface{}); ok {
			//setup the command as child of the former command
			var cmd = &cobra.Command{
				Use: key, Args: cobra.MaximumNArgs(0),
				Run: func(cmd *cobra.Command, args []string) {
					b, _ := json.MarshalIndent(tmp, "", "  ")
					fmt.Println(string(b))
				},
			}

			parent.AddCommand(cmd)
			setupConfigMap(tmp, cmd, accessor_)

		} else {

			tmp, _ := value.(ConfigEntry)
			//setup the command as child of the former command
			var cmd = &cobra.Command{
				Use: key, Short: tmp.Text, Args: cobra.MaximumNArgs(0),
				Run: func(cmd *cobra.Command, args []string) {

					if writeValue != "" {
						saveToConfig(writeValue, strings.Split(accessor_, "."))
					} else {
						if node {

						} else {
							fmt.Println(viper.Get(accessor_))
						}
					}
				},
			}
			//add all relevant flags
			cmd.Flags().BoolVarP(&node, "node", "n", false, "Return the value used by the active node")
			cmd.Flags().StringVarP(&writeValue, "write", "w", "", "Writed the value to the config file")
			parent.AddCommand(cmd)
		}
	}
}

func setupConfigDefaults(configs map[string]interface{}, accessor string) {

	for key, value := range configs {

		var accessor_ string
		if accessor == "" {
			accessor_ = key
		} else {
			accessor_ = accessor + "." + key
		}

		//it is is a map we need to go on iterating
		if tmp, ok := value.(map[string]interface{}); ok {
			setupConfigDefaults(tmp, accessor_)
		} else {
			entry := value.(ConfigEntry)
			viper.SetDefault(accessor_, entry.Default)
		}
	}
}

func getConfigMap() map[string]interface{} {

	if configDir == nil {
		fmt.Println("No config folder found, aborting")
	}
	var conf map[string]interface{}
	data, err := configDir.ReadFile("config.json")
	if err != nil {
		fmt.Println("Error while loading configuration file")
		return nil
	}
	json.Unmarshal(data, &conf)
	if conf != nil {
		return conf
	}

	return make(map[string]interface{})
}

func saveToConfigV(value interface{}, keys ...string) {

	//could be viper style key: connection.port
	if len(keys) == 1 {
		saveToConfig(value, strings.Split(keys[0], "."))
		return
	}

	//just a array of strings
	saveToConfig(value, keys)
}

func saveToConfig(value interface{}, keys []string) {

	conf := getConfigMap()

	//iterate over all nested values
	tmp := conf
	for i, key := range keys {
		if i == (len(keys) - 1) {
			tmp[key] = value
			break
		}
		//check if the map exists and create if not
		_, ok := tmp[key]
		if !ok {
			tmp[key] = make(map[string]interface{})
		}
		tmp = tmp[key].(map[string]interface{})
	}

	//write back the file
	data, _ := json.Marshal(conf)
	configDir.WriteFile("config.json", data)
}

func getDefaultNodeFolder() string {

	dir, err := homedir.Dir()
	if err != nil {
		panic("Unable to get home dir of user")
	}

	return filepath.Join(dir, ".ocp")
}
