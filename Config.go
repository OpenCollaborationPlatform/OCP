// Config
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/shibukawa/configdir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configDir *configdir.Config = nil

//Default init of config stuff:
// - There is always a config file, even if node is not initialized
// - If not existing it must be created
// - Default values are setup
// - Setup the subcommands for the main config command
func init() {

	configDirs := configdir.New("ocp", "")
	folders := configDirs.QueryFolders(configdir.Global)
	if len(folders) < 1 {
		fmt.Println("No folder for config found")
		return
	}

	configDir = folders[0]

	//we aways need to have a config file
	if !configDir.Exists("config.json") {
		if _, err := configDir.Create("config.json"); err != nil {
			log.Fatalf("Couldn't initialize the config file")
		}

		dummy := make(map[string]interface{})
		bytes, _ := json.Marshal(dummy)
		configDir.WriteFile("config.json", bytes)
	}

	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(configDir.Path)
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("While reading config file - \"%s\" \n", err)
	}

	//process the existing configs
	setupConfigMapCommands(configEntries, cmdConfig, "")
	setupConfigDefaults(configEntries, "")
}

//Default structure of the configs: types and values
//**************************************************

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
		"server": map[string]interface{}{
			"uri":  ConfigEntry{Default: "localhost", Short: "u", Text: "The server URI to which to conenct to (without port)"},
			"port": ConfigEntry{Default: 9000, Short: "p", Text: "The port which is used to connecto to the collaboration server"},
		},
		"p2p": map[string]interface{}{
			"port": ConfigEntry{Default: 7000, Short: "p", Text: "The port the node listens for p2p connections from other nodes"},
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

	log.Fatal("No such config entry exists")
	return ConfigEntry{}
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
		log.Fatalf("No flag can be created for config %s", accessor)
	}

	viper.BindPFlag(accessor, cmd.Flags().Lookup(name))
}

//Creates subcommands in the given parent that allow to read out and change all configs
func setupConfigMapCommands(value map[string]interface{}, parent *cobra.Command, accessor string) {

	if parent == nil {
		log.Fatal("Parent command is NIL")
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
			setupConfigMapCommands(tmp, cmd, accessor_)

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
	data, _ := json.MarshalIndent(conf, "", "  ")
	configDir.WriteFile("config.json", data)
}

func getDefaultNodeFolder() string {

	dir, err := homedir.Dir()
	if err != nil {
		log.Fatal("Unable to get home dir of user")
	}

	return filepath.Join(dir, ".ocp")
}
