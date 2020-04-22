// Node.go
package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/document"
	"github.com/ickby/CollaborationNode/user"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/utils"

)

const (
	OcpVersion string = "v0.1 development version"
)

type Node struct {
	//connection
	quit    chan string        //This is the quit channel: send on it to shutdown
	Router  *connection.Router //WAMP router for client connections (and gateway)
	Host    *p2p.Host          //P2P host for direct comunication and data transfer

	//functionality
	Documents *document.DocumentHandler //the handler for documents
	Users     *user.UserHandler //handler for users

	//misc
	Version string             //Default setup version string
}

func NewNode() *Node {

	return &Node{
		quit:    make(chan string),
		Version: OcpVersion}
}

func (n *Node) Start() error {

	log.Printf("Sartup OCP Node %s\n", n.Version)

	//setup the p2p network
	n.Host = p2p.NewHost()
	err := n.Host.Start(true)
	if err != nil {
		return utils.StackError(err, "Cannot startup Node")
	}

	//start up our local router
	n.Router = connection.NewRouter()
	n.Router.Start(n.quit)

	//load the document component
	dh, err := document.NewDocumentHandler(n.Router, n.Host) 
	if err != nil {
		return utils.StackError(err, "Unable to load document handler")
	}
	n.Documents = dh

	//load the user component
	uh, err := user.NewUserHandler(n.Router, n.Host) 
	if err != nil {
		return utils.StackError(err, "Unable to load user handler")
	}
	n.Users = uh

	//make sure we get system signals
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		n.quit <- fmt.Sprintf("received system signal \"%s\"", sig)
	}()

	//save the pidfile
	err = utils.WritePidPort()
	if err != nil {
		return utils.StackError(err, "Unable to write pid/port file")
	}

	log.Println("OCP node ready")
	
	return nil
}

func (n *Node) Stop(ctx context.Context, reason string) {

	n.Users.Close()
	n.Documents.Close(ctx)
	n.Host.Stop(ctx)
	n.Router.Stop()
	utils.ClearPidPort()
	defer func() { n.quit <- reason }()
}

func (n *Node) WaitForStop() {

	reason := <-n.quit
	log.Printf("Shuting down: %s", reason)
}
