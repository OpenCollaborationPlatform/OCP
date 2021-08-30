package commands

import (
	"context"
	"fmt"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	nxclient "github.com/gammazero/nexus/v3/client"
	wamp "github.com/gammazero/nexus/v3/wamp"
	"github.com/spf13/cobra"
)

//flag variables
var ()

func init() {

	cmdDocClose.Flags().BoolP("remove", "r", false, "removes all data and folders of docuents")
	cmdDocClose.Flags().BoolP("all", "a", false, "closes all open documens")

	cmdDocuments.AddCommand(cmdDocClose, cmdDocCreate, cmdDocOpen, cmdDocContent)
	rootCmd.AddCommand(cmdDocuments)
}

var cmdDocuments = &cobra.Command{
	Use:   "documents",
	Short: "Create, access and manipulate documents",
	Long:  `Create, access and manipulate documents`,
	Run: onlineCommand("documents", func(ctx context.Context, args []string, flags map[string]interface{}) string {

		docs := ocpNode.Documents.ListDocuments()

		result := fmt.Sprintln("Currently open documents:")
		for _, doc := range docs {
			result += doc + "\n"
		}

		result += "\nInvited for documents:\n"
		for _, doc := range ocpNode.Documents.Invitations() {
			result += doc + "\n"
		}

		return result
	}),
}

var cmdDocClose = &cobra.Command{
	Use:   "close [id]",
	Short: "close documents that are currently open",
	Args:  cobra.ExactArgs(1),
	Run: onlineCommand("documents.close", func(ctx context.Context, args []string, flags map[string]interface{}) string {

		docs := args
		if flags["all"].(bool) {
			docs = ocpNode.Documents.ListDocuments()
		}

		for _, doc := range docs {
			err := ocpNode.Documents.CloseDocument(ctx, doc)
			if err != nil {
				return err.Error()
			}
		}

		return "sucessfully closed"
	}),
}

var cmdDocOpen = &cobra.Command{
	Use:   "open [id]",
	Short: "Open a arbitrary documents that is currently not opened in the node",
	Long: `Finds and connects other peers for the given document and opens the doc. Note that it must be allowed 
			for us to join the document, meaning the other document peers must have called addPeer for us`,
	Args: cobra.ExactArgs(1),
	Run: onlineCommand("documents.open", func(ctx context.Context, args []string, flags map[string]interface{}) string {

		err := ocpNode.Documents.OpenDocument(ctx, args[0])
		if err != nil {
			return err.Error()
		}
		return "sucessfully opened"
	}),
}

var cmdDocCreate = &cobra.Command{
	Use:   "create [dmlpath]",
	Short: "Creates a new document with the dml structure given in the link (link must be toplevel Dml folder)",
	Args:  cobra.ExactArgs(1),
	Run: onlineCommand("documents.create", func(ctx context.Context, args []string, flags map[string]interface{}) string {

		doc, err := ocpNode.Documents.CreateDocument(ctx, args[0])
		if err != nil {
			return err.Error()
		}
		return doc.ID
	}),
}

var cmdDocContent = &cobra.Command{
	Use:   "content [id] [path] fncArguments",
	Short: "Accesses the document content with the given path and arguments, e.g. call mydocid MyToplevel.MySubObject.myFunction arg1",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		if !isConnected {
			fmt.Println("No node is currently running. Aborting.")
			return
		}

		path := "ocp.documents." + args[0] + ".content." + args[1]

		wampArgs := make(wamp.List, len(args)-2)
		for i, arg := range args[2:] {
			wampArgs[i] = arg
		}
		result, err := nodeClient.Call(cmd.Context(), path, wamp.Dict{}, wampArgs, wamp.Dict{}, nil)
		if err != nil {
			rpcerr, ok := err.(nxclient.RPCError)
			if !ok {
				fmt.Println(err.Error())
			}
			ocperr := utils.WampRPCErrorToError(rpcerr)
			if verbose {
				fmt.Println(ocperr.ErrorWithStacktrace())
			} else {
				fmt.Println(ocperr.Error())
			}
			return
		}
		for _, res := range result.Arguments {
			fmt.Println(res)
		}
	},
}
