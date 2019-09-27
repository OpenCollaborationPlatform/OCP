package commands

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

//flag variables
var (

)

func init() {

	rootCmd.AddCommand(cmdDocuments)
}

var cmdDocuments = &cobra.Command{
	Use:   "documents",
	Short: "Create, access and manipulate documents",
	Long: `Create, access and manipulate documents`,
	Run: onlineCommand("documents", func(ctx context.Context, args []string, flags map[string]interface{}) string {
		
		docs := ocpNode.Documents.ListDocuments()
		
		result := fmt.Sprintln("Currently open documents")
		for _, doc := range docs {
			result += doc + "\n"
		}
		
		return result
	}),
}
