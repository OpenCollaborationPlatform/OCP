package main

import (
	"log"
	"os"

	"github.com/OpenCollaborationPlatform/OCP/commands"
)

func main() {

	//set the main logging output
	log.SetOutput(os.Stdout)

	//execute OCP
	commands.Execute()
}
