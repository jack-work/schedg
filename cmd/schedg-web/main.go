package main

import (
	"flag"
	"log"

	"github.com/jack-work/schedg/internal/config"
	"github.com/jack-work/schedg/internal/webserver"
)

func main() {
	addr := flag.String("addr", ":9746", "listen address")
	webDir := flag.String("web", "web/dist", "path to frontend build output")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load schedg config: %v", err)
	}

	srv := webserver.New(cfg, *webDir)
	log.Fatal(srv.ListenAndServe(*addr))
}
