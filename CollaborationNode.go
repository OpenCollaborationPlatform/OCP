package main

import (
	"github.com/ickby/CollaborationNode/commands"
	"log"
	"os"
)

func main() {

	//set the main logging output
	log.SetOutput(os.Stdout)

	//execute OCP
	commands.Execute()
}
