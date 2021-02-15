package main

import (
	"log"
	"os"

	"github.com/ickby/CollaborationNode/commands"
)

func main() {

	//set the main logging output
	log.SetOutput(os.Stdout)

	//execute OCP
	commands.Execute()
}
