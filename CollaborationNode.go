package main

import (
	"CollaborationNode/connection"
	"CollaborationNode/document"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/viper"

	"github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p-peer"
)

var (
	quit   chan string //this is the quit channel
	server *connection.Server
	router *connection.Router
	nodeID peer.ID
)

func startup() {

	if isConnected {
		fmt.Println("OCP node is already running. There cannot be a second node.")
		return
	}

	content, err := ioutil.ReadFile(filepath.Join(viper.GetString("directory"), "public"))
	if err != nil {
		log.Fatalf("Public key could not be read: %s\n", err)
	}
	key, err := crypto.UnmarshalPublicKey(content)
	if err != nil {
		log.Fatalf("Public key is invalid: %s\n", err)
	}
	nodeID, _ := peer.IDFromPublicKey(key)

	//connect to the collaboration server
	server = connection.NewServer(nodeID)
	if err := server.Start(quit); err != nil {
		log.Printf("Connection to server failed: %s", err)
	} else {
		log.Print("Connection to server successfull")
	}
	defer server.Stop()

	//start up our local router
	router = connection.NewRouter(server)
	router.Start(quit)
	defer router.Stop()

	//load the document component
	document.Setup(server, router, nodeID)

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
		log.Fatalf(fmt.Sprintf("Writing PID file failed with \"%s\"", err))
	}
	defer ClearPidPort()

	//setup all commands
	initOnlineCommands()

	reason := <-quit
	log.Printf("Shuting down: %s", reason)
}

func main() {

	quit = make(chan string)

	cmd := getCommands()
	cmd.Execute()
}
