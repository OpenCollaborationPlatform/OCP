package main

import (
	"CollaborationNode/connection"
	"CollaborationNode/document"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gammazero/nexus/client"
	"github.com/gammazero/nexus/wamp"
	"github.com/satori/go.uuid"
)

var (
	quit   chan string //this is the quit channel
	server *connection.Server
	router *connection.Router
	nodeID uuid.UUID
)

func startup() {

	if isConnected {
		fmt.Println("OCP node is already running. There cannot be a second node.")
		return
	}

	nodeID = uuid.NewV4()

	//connect to the collaboration server
	server := connection.Server{}
	server.Start(quit)
	defer server.Stop()

	//start up our local router
	router := connection.NewRouter(&server)
	router.Start(quit)
	defer router.Stop()

	//load the document component
	document.Setup(&server, router)

	//make the node stoppable by command
	client, err := router.GetLocalClient("command")
	if err != nil {
		panic(fmt.Sprintf("Unable to setup command client: %s", err))
	}

	if err := client.Register("ocp.command.stop", shutDown, nil); err != nil {
		panic(err)
	}

	//make sure we get system signals
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		quit <- fmt.Sprintf("received system signal \"%s\"", sig)
	}()

	//save the pidfile
	err = WritePidPort()
	if err != nil {
		panic(fmt.Sprintf("Writing PID file failed with \"%s\"", err))
	}
	defer ClearPidPort()

	reason := <-quit
	log.Printf("Shuting down, reason: %s", reason)
}

func shutDown(ctx context.Context, args wamp.List, kwargs, details wamp.Dict) *client.InvokeResult {

	defer func() { quit <- "Shutdown request received" }()
	return &client.InvokeResult{}
}

func main() {

	quit = make(chan string)
	initConfig()

	cmd := getCommands()
	cmd.Execute()
}
