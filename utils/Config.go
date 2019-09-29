// Config
package utils

import (
	"strconv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/shibukawa/configdir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	configEntries = map[string]interface{}{
		"connection": map[string]interface{}{
			"uri": ConfigEntry{Default: "localhost", Short: "u", Text: "The uri the node is listening on for client connections "},
			"port": ConfigEntry{Default: 8000, Short: "p", Text: "The port on which the node listents for client connections"},
		},
		"p2p": map[string]interface{}{
			"port":      ConfigEntry{Default: 7000, Short: "p", Text: "The port the node listens on for p2p connections from other nodes"},
			"uri":       ConfigEntry{Default: "0.0.0.0", Short: "u", Text: "The adress the node listens on for p2p connections from other nodes (without port)"},
			"bootstrap": ConfigEntry{Default: []string{"/ip4/167.99.243.88/tcp/7000/ipfs/Qma3YcKHuurgq99grLoGhBLBXtzpjSpPRscZ8BZT1K2HZi"}, Short: "b", Text: "The nodes to connect to at startup for adress indetification"},
			"only": ConfigEntry{Default: false, Short: "o", Text: "The node bootstraps only the config nodes, not the default ones"},
		},
	}
)


//Default init of config stuff:
// - There is always a config file, even if node is not initialized
// - If not existing it must be created
// - Default values are setup
// - Setup the subcommands for the main config command
func InitConfig(path string) {

	configDirs := configdir.New("ocp", "")
	folders := configDirs.QueryFolders(configdir.Global)
	if len(folders) < 1 {
		fmt.Println("No folder for config found")
		return
	}
	ConfigDir := folders[0]

	if path == "default" {

		//we aways need to have a config file
		if !ConfigDir.Exists("config.json") {
			if _, err := ConfigDir.Create("config.json"); err != nil {
				log.Fatalf("Couldn't initialize the config file")
			}

			dummy := make(map[string]interface{})
			bytes, _ := json.Marshal(dummy)
			ConfigDir.WriteFile("config.json", bytes)
		}

		viper.SetConfigName("config")
		viper.SetConfigType("json")
		viper.AddConfigPath(ConfigDir.Path)

	} else {

		//check if it is a full path or a config file name
		dir, file := filepath.Split(path)
		parts := strings.Split(file, ".")

		if dir == "" && len(parts) == 1 {
			//we load from default forlder, but special name
			if !ConfigDir.Exists(parts[0] + ".json") {
				log.Fatalf("Given config file does not exist")
			}

			viper.SetConfigName(parts[0])
			viper.SetConfigType("json")
			viper.AddConfigPath(ConfigDir.Path)

		} else {
			//full file path is given
			if _, err := os.Stat(path); os.IsNotExist(err) {
				log.Fatalln("Invalid path to config file")
			}

			viper.SetConfigName(parts[0])
			viper.SetConfigType(parts[1])
			viper.AddConfigPath(dir)
		}
	}

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("While reading config file: %s", err)
	}

	//process the existing configs
	setupConfigDefaults(configEntries, "")
}

//Default structure of the configs: types and values
//**************************************************

type ConfigEntry struct {
	Short   string
	Text    string
	Default interface{}
}

func (self ConfigEntry) ValueFromString(value string) (interface{}, error) {
	
	switch self.Default.(type) {
	case int:
		i, err := strconv.ParseInt(value, 10, 32)
		return int(i), err
		
	case string:
		return value, nil
		
	case float64:
		return strconv.ParseFloat(value, 64)
		
	case []string:
		//first and last are [], remove!
		list := value[1:(len(value)-1)]
		return strings.Split(list, ","), nil
		
	case []int:
		res := make([]int, 0)
		//first and last are [], remove!
		list := value[1:(len(value)-1)]
		splits := strings.Split(list, ",")
		for _, val := range splits {
			i, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				res = append(res, int(i))
			}
		}
		return res, nil
		
	case bool:
		if strings.ToLower(value) == "true" {
			return true, nil
		}
		if strings.ToLower(value) == "false" {
			return false, nil
		}

	default:
		return nil, fmt.Errorf("error")
	}
	
	return nil, fmt.Errorf("Cannot read, need type %T", self.Default)
}

func (self ConfigEntry) IsStringSlice() bool {

	switch self.Default.(type) {
		case []string: 
			return true
		default:
			return false 
	}
	return false
}

func (self ConfigEntry) IsIntSlice() bool {

	switch self.Default.(type) {
		case []int: 
			return true
		default:
			return false 
	}
	return false
}

func (self ConfigEntry) IsSlice() bool {

	return self.IsStringSlice() || self.IsIntSlice()
}

func GetConfigEntry(keys ...string) (ConfigEntry, error) {

	//could be viper access string like connection.port
	if len(keys) == 1 {
		return getConfigEntryByArray(strings.Split(keys[0], "."))
	}

	return getConfigEntryByArray(keys)
}

func getConfigEntryByArray(keys []string) (ConfigEntry, error) {

	//iterate over all nested values
	tmp := configEntries
	for i, key := range keys {
		
		val, ok := tmp[key]
		if !ok {
			return ConfigEntry{}, fmt.Errorf("Key %v does not exist in default config", key)
		}
		
		if i == (len(keys) - 1) {
			//maybe its not a entry...
			if !ok || val == nil {
				return ConfigEntry{}, fmt.Errorf("Default config entry is empty, which is invalid")
			}
			return val.(ConfigEntry), nil
		}
		tmp = tmp[key].(map[string]interface{})
	}

	log.Fatal("No such config entry exists")
	return ConfigEntry{}, nil
}

//Adds a flag to a command based on a config entry. This uses the long and short names
//as defined in the config, as well as the default vaule. The accessor string is the standart
//viper access string.
func AddConfigFlag(cmd *cobra.Command, accessor string) {

	config, err := GetConfigEntry(accessor)
	if err != nil {
		return
	}
	
	keys := strings.Split(accessor, ".")
	name := keys[len(keys)-1]

	switch config.Default.(type) {
	case int:
		cmd.Flags().IntP(name, config.Short, viper.GetInt(accessor), config.Text)
	case string:
		cmd.Flags().StringP(name, config.Short, viper.GetString(accessor), config.Text)
	case float64:
		cmd.Flags().Float64P(name, config.Short, viper.GetFloat64(accessor), config.Text)
	case []string:
		cmd.Flags().StringSlice(name, viper.GetStringSlice(accessor), config.Text)
	default:
		log.Fatalf("No flag can be created for config %s", accessor)
	}

	viper.BindPFlag(accessor, cmd.Flags().Lookup(name))
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


func GetDefaultNodeFolder() string {

	dir, err := homedir.Dir()
	if err != nil {
		log.Fatal("Unable to get home dir of user")
	}

	return filepath.Join(dir, ".ocp")
}

func CreateConfigFile(name string) (string, error) {

	dir, file := filepath.Split(name)
	if dir != "" {
		return "", fmt.Errorf("Config file name is not allowed to contain a path")
	}
	if file == "" {
		return "", fmt.Errorf("Name is invalid")
	}

	parts := strings.Split(file, ".")
	if len(parts) > 1 && parts[2] != "json" {
		return "", fmt.Errorf("No file ending is allowd exept json. Best to provide name only")
	}

	configDirs := configdir.New("ocp", "")
	folders := configDirs.QueryFolders(configdir.Global)
	if len(folders) < 1 {
		return "", fmt.Errorf("No folder for config found")
	}

	ConfigDir := folders[0]

	//Only single file per name
	if ConfigDir.Exists(parts[0] + ".json") {
		return "", fmt.Errorf("Config file already exists")
	}

	//now we can fiinally create it
	if _, err := ConfigDir.Create(parts[0] + ".json"); err != nil {
		return "", fmt.Errorf("Couldn't create the config file: %v", err)
	}

	//setup a readable dummy
	dummy := make(map[string]interface{})
	bytes, _ := json.Marshal(dummy)
	ConfigDir.WriteFile(parts[0]+".json", bytes)

	return parts[0], nil
}

func RemoveConfigFile(name string) error {

	parts := strings.Split(name, ".")
	if len(parts) > 1 && parts[2] != "json" {
		return fmt.Errorf("Config files are always .json")
	}

	configDirs := configdir.New("ocp", "")
	folders := configDirs.QueryFolders(configdir.Global)
	if len(folders) < 1 {
		return fmt.Errorf("No folder for config found")
	}

	ConfigDir := folders[0]

	//Only single file per name
	if !ConfigDir.Exists(parts[0] + ".json") {
		return fmt.Errorf("Config file does not exist")
	}

	//now we can finally remove it
	path := filepath.Join(ConfigDir.Path, parts[0]+".json")
	err := os.Remove(path)

	return err
}
