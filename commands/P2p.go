// p2p.go
package commands

import (
	"CollaborationNode/p2p"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"strings"

	"github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p-peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/spf13/cobra"
)

func init() {

	//flags
	cmdP2PPeers.Flags().BoolP("address", "a", false, "Print full adress instead of ID (only one of possibly multiple)")
	cmdP2PSwarmCreate.Flags().IntP("seed", "s", 0, "set a seed for swarm key generation for deterministic outcomes instead of random keys")
	cmdP2PSwarmCreate.Flags().BoolP("public", "p", false, "make the swarm publically accessible")

	cmdP2PSwarm.AddCommand(cmdP2PSwarmCreate, cmdP2PSwarmAdd, cmdP2PSwarmEvent)
	cmdP2P.AddCommand(cmdP2PPeers, cmdP2PAddrs, cmdP2PConnect, cmdP2PClose, cmdP2PSwarm)
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
				result += fmt.Sprintf("%s\n", addrs[0].String())
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
	Short: "close [peer] Close connection to given peer",
	Args:  cobra.ExactArgs(1),

	Run: onlineCommand("p2p.close", func(args []string, flags map[string]interface{}) string {

		pid, err := p2p.PeerIDFromString(args[0])
		if err != nil {
			return err.Error()
		}
		err = ocpNode.Host.CloseConnection(pid)
		if err != nil {
			return err.Error()
		}
		return "Connection successfully closed"
	}),
}

var cmdP2PSwarm = &cobra.Command{
	Use:   "swarm",
	Short: "lists all open swarms and allows to handle them via subcommands",
	Args:  cobra.ExactArgs(0),

	Run: onlineCommand("p2p.swarm", func(args []string, flags map[string]interface{}) string {

		var result string
		for _, swarm := range ocpNode.Host.Swarms() {

			result += swarm.ID.Pretty()
			if swarm.IsPublic() {
				result += " (public)"
			} else {
				result += " (private)"
			}
		}
		return result
	}),
}

var cmdP2PSwarmCreate = &cobra.Command{
	Use:   "create",
	Short: "create [id] [options] creates a new swarm with given id",
	Args:  cobra.ExactArgs(1),

	Run: onlineCommand("p2p.swarm.create", func(args []string, flags map[string]interface{}) string {

		seed := int(flags["seed"].(float64))
		var r io.Reader
		if seed == 0 {
			r = rand.Reader
		} else {
			r = mrand.New(mrand.NewSource(int64(seed)))
		}
		priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, r)
		if err != nil {
			return fmt.Sprintf("Error at key generation: %s", err)
		}

		ocpNode.Host.CreateSwarm(p2p.SwarmID(args[0]), priv, flags["public"].(bool))
		return "Successfull created swarm"
	}),
}

var cmdP2PSwarmAdd = &cobra.Command{
	Use:   "add",
	Short: "add [swarm] [peer] adds a peer to a swarm",
	Args:  cobra.ExactArgs(2),

	Run: onlineCommand("p2p.swarm.add", func(args []string, flags map[string]interface{}) string {

		sid := p2p.SwarmID(args[0])
		pid, err := p2p.PeerIDFromString(args[1])
		if err != nil {
			return fmt.Sprintf("Error reading PeerID: %s", err)
		}

		if !ocpNode.Host.IsConnected(pid) {
			return "Peer is not connected, can't be added"
		}

		swarm, err := ocpNode.Host.GetSwarm(sid)
		if err != nil {
			return err.Error()
		}

		if err := swarm.AddPeer(pid, false); err != nil {
			return fmt.Sprintf("Error adding PeerID to swarm: %s", err)
		}

		return "Successfully added peer"
	}),
}

var cmdP2PSwarmEvent = &cobra.Command{
	Use:   "event",
	Short: "event [swarm] [uri] [kwargs] send a event with given keyword arguments into swarm",
	Args:  cobra.MinimumNArgs(2),

	Run: onlineCommand("p2p.swarm.event", func(args []string, flags map[string]interface{}) string {

		sid := p2p.SwarmID(args[0])
		swarm, err := ocpNode.Host.GetSwarm(sid)
		if err != nil {
			return err.Error()
		}

		//build the keyword arguments
		d := p2p.Dict{}
		for _, arg := range args[2:] {
			parts := strings.Split(arg, ":")
			if len(parts) != 2 {
				return "Arguments must be of form name:value"
			}
			d[parts[0]] = parts[1]
		}

		swarm.PostEvent(args[1], d)
		return "Successfully posted event"
	}),
}
