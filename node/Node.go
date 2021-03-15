// Node.go
package node

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/viper"

	"github.com/ickby/CollaborationNode/connection"
	"github.com/ickby/CollaborationNode/document"
	"github.com/ickby/CollaborationNode/p2p"
	"github.com/ickby/CollaborationNode/user"
	"github.com/ickby/CollaborationNode/utils"

	hclog "github.com/hashicorp/go-hclog"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	OcpVersion string = "v0.1 development version"
)

type Node struct {
	//connection
	quit   chan string        //This is the quit channel: send on it to shutdown
	Router *connection.Router //WAMP router for client connections (and gateway)
	Host   *p2p.Host          //P2P host for direct comunication and data transfer

	//functionality
	Documents *document.DocumentHandler //the handler for documents
	Users     *user.UserHandler         //handler for users
	Config    *utils.ConfigHandler      //handler for config API

	//misc
	logger  hclog.Logger
	Version string //Default setup version string
}

func NewNode() *Node {

	return &Node{
		quit:    make(chan string),
		Version: OcpVersion,
		logger:  nil}
}

func (self *Node) Start() error {

	//setup logging system
	var writer io.Writer = os.Stdout
	if viper.GetBool("log.file.enable") {
		writer = &lumberjack.Logger{
			Filename:   filepath.Join(viper.GetString("directory"), "Logs", "ocp.log"),
			MaxSize:    viper.GetInt("log.file.size"),    // megabytes
			MaxBackups: viper.GetInt("log.sile.backups"), //num
			MaxAge:     viper.GetInt("log.file.age"),     //days
		}
	}
	self.logger = hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(viper.GetString("log.level")),
		JSONFormat: viper.GetBool("log.json"),
		Output:     writer,
	})

	self.logger.Info("Sartup OCP Node", "version", self.Version)

	//start up our local router
	self.Router = connection.NewRouter(self.logger.Named("API"))
	err := self.Router.Start(self.quit)
	if err != nil {
		return err
	}

	//setup the p2p network
	self.Host = p2p.NewHost(self.Router, self.logger.Named("P2P"))
	err = self.Host.Start(true)
	if err != nil {
		err = utils.StackError(err, "Cannot startup p2p host")
		self.logger.Error(err.Error())
		return err
	}

	//load the document component
	dh, err := document.NewDocumentHandler(self.Router, self.Host, self.logger.Named("DocHandler"))
	if err != nil {
		err = utils.StackError(err, "Unable to load document handler")
		self.logger.Error(err.Error())
		return err
	}
	self.Documents = dh

	//load the user component
	uh, err := user.NewUserHandler(self.Router, self.Host)
	if err != nil {
		err = utils.StackError(err, "Unable to load user handler")
		self.logger.Error(err.Error())
		return err
	}
	self.Users = uh

	//add config API
	client, _ := self.Router.GetLocalClient("config")
	self.Config = utils.NewConfigAPI(client)

	//make sure we get system signals
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		self.quit <- fmt.Sprintf("received system signal \"%s\"", sig)
	}()

	//save the pidfile
	err = utils.WritePidPort()
	if err != nil {
		err = utils.StackError(err, "Unable to write pid/port file")
		self.logger.Error(err.Error())
		return err
	}

	self.logger.Info("Node ready")

	return nil
}

func (self *Node) Stop(ctx context.Context, reason string) {

	self.Users.Close()
	self.Documents.Close(ctx)
	self.Host.Stop(ctx)
	self.Router.Stop()
	utils.ClearPidPort()
	defer func() { self.quit <- reason }()
}

func (self *Node) WaitForStop() {

	reason := <-self.quit
	self.logger.Info("Shuting down", "reason", reason)
}
