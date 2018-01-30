// Node.go
package node

import (
	"CollaborationNode/connection"
	"CollaborationNode/document"
	"CollaborationNode/p2p"
	"CollaborationNode/utils"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/viper"
)

type Node struct {
	quit    chan string        //This is the quit channel: send on it to shutdown
	Server  *connection.Server //Connection to WAMP server
	Router  *connection.Router //WAMP router for client connections (and gateway)
	Host    *p2p.Host          //P2P host for direct comunication and data transfer
	ID      p2p.PeerID         //n.ID: setup from config
	Version string             //Default setup version string
}

func NewNode() *Node {

	return &Node{
		quit:    make(chan string),
		Version: "v0.1 development version"}
}

func (n *Node) Start() {

	//load ID and establish the p2p network
	n.ID = p2p.LoadPeerIDFromPublicKeyFile(filepath.Join(viper.GetString("directory"), "public"))
	log.Printf("Node ID set to %s", n.ID.Pretty())
	n.Host = p2p.NewHost()
	n.Host.Start()

	//connect to the collaboration server
	n.Server = connection.NewServer(n.ID)
	if err := n.Server.Start(n.quit); err != nil {
		log.Printf("Connection to server failed: %s", err)
	} else {
		log.Print("Connection to server successfull")
	}

	//start up our local router
	n.Router = connection.NewRouter(n.Server)
	n.Router.Start(n.quit)

	//load the document component
	document.Setup(n.Server, n.Router, n.ID)

	//make sure we get system signals
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		n.quit <- fmt.Sprintf("received system signal \"%s\"", sig)
	}()

	//save the pidfile
	err := utils.WritePidPort()
	if err != nil {
		log.Fatalf(fmt.Sprintf("Writing PID file failed with \"%s\"", err))
	}
}

func (n *Node) Stop(reason string) {

	n.Host.Stop()
	n.Server.Stop()
	n.Router.Stop()
	utils.ClearPidPort()
	defer func() { n.quit <- reason }()
}

func (n *Node) WaitForStop() {

	reason := <-n.quit
	log.Printf("Shuting down: %s", reason)
}
