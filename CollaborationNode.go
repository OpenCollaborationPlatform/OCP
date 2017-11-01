package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/beatgammit/turnpike"
)

var (
	quit chan string //this is the quit channel
)

func startup() {

	if isConnected {
		fmt.Println("OCP node is already running. There cannot be a second node.")
		return
	}

	//connect to the collaboration server
	cs := CollaborationServer{}
	cs.start(quit)
	defer cs.stop()

	//start up our server
	ls := LocalServer{collab: &cs}
	ls.start(quit)
	defer ls.stop()

	//make the node stoppable by command
	if client, err := ls.getClient(); err != nil {
		panic(err)
	} else if err := client.BasicRegister("ocp.command.stop", shutDown); err != nil {
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
	err := WritePidPort()
	if err != nil {
		panic(fmt.Sprintf("Writing PID file failed with \"%s\"", err))
	}
	defer ClearPidPort()

	reason := <-quit
	log.Printf("Shuting down, reason: %s", reason)
}

func shutDown(args []interface{}, kwargs map[string]interface{}) (result *turnpike.CallResult) {

	defer func() { quit <- "Shutdown request received" }()
	return &turnpike.CallResult{}
}

func main() {

	quit = make(chan string)
	initConfig()

	cmd := getCommands()
	cmd.Execute()
}
