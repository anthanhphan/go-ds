package main

import (
	"flag"
	"log"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/app"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "configPath", "", "Path to configuration file")
	flag.Parse()

	application, err := app.New(configPath)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	if err := application.Run(); err != nil {
		log.Fatalf("Application failed: %v", err)
	}
}
