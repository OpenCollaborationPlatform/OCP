// p2p.go
package commands

import (
	"CollaborationNode/p2p"
	"fmt"

	"github.com/libp2p/go-libp2p-peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/spf13/cobra"
)

func init() {

	//flags
	cmdP2PPeers.Flags().BoolP("address", "a", false, "Print full adress instead of ID (only one of possibly multiple)")

	cmdP2P.AddCommand(cmdP2PPeers, cmdP2PAddrs, cmdP2PConnect, cmdP2PClose)
	rootCmd.AddCommand(cmdP2P)
}

var cmdP2P = &cobra.Command{
	Use:   "p2p",
	Short: "Access information about the p2p network",

	Run: onlineCommand("p2p", func(args []string, flags map[string]interface{}) string {

		result := fmt.Sprintf("Connected Peers:\t%d\n", len(ocpNode.Host.Peers()))
		result += fmt.Sprintln("Own addresses:")
		for _, addr := range ocpNode.Host.OwnAddresses() {
			result += addr.String() + "\n"
		}
		return result
	}),
}

var cmdP2PPeers = &cobra.Command{
	Use:   "peers",
	Short: "List all peers the node is connected to",

	Run: onlineCommand("p2p.peers", func(args []string, flags map[string]interface{}) string {

		result := ""
		for _, peer := range ocpNode.Host.Peers() {

			if flags["address"].(bool) {
				addrs, err := ocpNode.Host.Addresses(peer)
				if err != nil {
					return fmt.Sprintf("Error while parsing adresses: %s", err)
				}
				result += fmt.Sprintf("%s", addrs[0].String())
			} else {
				result += peer.Pretty() + "\n"
			}
		}
		return result
	}),
}

var cmdP2PAddrs = &cobra.Command{
	Use:   "address",
	Short: "List all known addresses for the given peer",
	Args:  cobra.ExactArgs(1),

	Run: onlineCommand("p2p.address", func(args []string, flags map[string]interface{}) string {

		peerid, err := peer.IDB58Decode(args[0])
		if err != nil {
			return err.Error()
		}

		result := ""
		addrs, err := ocpNode.Host.Addresses(p2p.PeerID{peerid})
		if err != nil {
			result = fmt.Sprintf("Error with peer ID: %s", err)
			return result
		}
		for _, addr := range addrs {
			result += addr.String() + "\n"
		}

		return result
	}),
}

var cmdP2PConnect = &cobra.Command{
	Use:   "connect",
	Short: "Connect to peer with given full address (e.g. /ip4/1.2.3.4/tcp/10/ipfs/Qxml...)",
	Args:  cobra.ExactArgs(1),

	Run: onlineCommand("p2p.connect", func(args []string, flags map[string]interface{}) string {

		addr, err := multiaddr.NewMultiaddr(args[0])
		if err != nil {
			return err.Error()
		}

		if err := ocpNode.Host.Connect(addr); err != nil {
			return err.Error()
		}

		return "Successfully connected"
	}),
}

var cmdP2PClose = &cobra.Command{
	Use:   "close",
	Short: "List all peers the node is connected to",

	Run: onlineCommand("p2p.close", func(args []string, flags map[string]interface{}) string {

		result := fmt.Sprintf("%v", ocpNode.Host.Peers())
		return result
	}),
}
