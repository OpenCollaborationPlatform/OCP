package commands

import (
	"context"
	"github.com/spf13/cobra"

	"github.com/ickby/CollaborationNode/user"
)

//flag variables
var (
)

func init() {

	cmdUsers.Flags().StringP("set", "s", "", "Set a new user name for this node")
	cmdUsersFind.Flags().IntP("number", "n", 1, "How many peers should be found for the user")
	
	cmdUsers.AddCommand(cmdUsersFind)
	rootCmd.AddCommand(cmdUsers)
}

var cmdUsers = &cobra.Command{
	Use:   "user",
	Short: "Set own user identity and find others",
	Run: onlineCommand("users", func(ctx context.Context, args []string, flags map[string]interface{}) string {
		
	
		newname := flags["set"]
		if newname != "" {
			err := ocpNode.Users.SetUser(ctx, user.UserID(newname.(string)))
			if err != nil {
				return err.Error()
			}
			return "New user successfully set"
		}

		id := ocpNode.Users.ID() 		
		if id == "" {
			return "No user name set"
		}
		return id.Pretty()
	}),
}

var cmdUsersFind = &cobra.Command{
	Use:   "find",
	Short: "Find peer id for given user, searches till correct amount of nodes is found or command times out",
	Run: onlineCommand("users.find", func(ctx context.Context, args []string, flags map[string]interface{}) string {
		
		if len(args) != 1 {
			return "Argument must be user name"
		}
		num := int(flags["number"].(uint64))
		
		user, err := ocpNode.Users.FindUser(ctx, user.UserID(args[0]), num)
		if err != nil {
			return err.Error()
		}
		return user.Pretty()
	}),
}

