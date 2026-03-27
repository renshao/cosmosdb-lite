package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/shaoren/cosmosdb-lite/internal/api"
	"github.com/shaoren/cosmosdb-lite/internal/cert"
	"github.com/shaoren/cosmosdb-lite/internal/store"
)

func main() {
	port := flag.Int("port", 8081, "HTTPS port")
	dataDir := flag.String("data-dir", "", "Directory for persistent storage (default: in-memory only)")
	certDir := flag.String("cert-dir", defaultCertDir(), "Directory for TLS cert/key")
	noAuth := flag.Bool("no-auth", false, "Disable auth validation")
	logLevel := flag.String("log-level", "info", "Log verbosity: debug, info, warn, error")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if *logLevel == "debug" {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	}

	// Initialize storage
	var s store.Store
	if *dataDir != "" {
		var err error
		s, err = store.NewMemoryStore(*dataDir)
		if err != nil {
			log.Fatalf("Failed to initialize store with data-dir %s: %v", *dataDir, err)
		}
		log.Printf("Persistent storage enabled at: %s", *dataDir)
	} else {
		var err error
		s, err = store.NewMemoryStore("")
		if err != nil {
			log.Fatalf("Failed to initialize store: %v", err)
		}
		log.Println("Using in-memory storage (data will be lost on restart)")
	}

	// Ensure cert directory exists
	if err := os.MkdirAll(*certDir, 0755); err != nil {
		log.Fatalf("Failed to create cert directory: %v", err)
	}

	// Generate or load TLS certificate
	certFile := filepath.Join(*certDir, "cosmosdb-lite.crt")
	keyFile := filepath.Join(*certDir, "cosmosdb-lite.key")
	if err := cert.EnsureCert(certFile, keyFile); err != nil {
		log.Fatalf("Failed to generate TLS certificate: %v", err)
	}
	log.Printf("TLS certificate: %s", certFile)
	log.Printf("TLS private key: %s", keyFile)

	// Print startup info
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              CosmosDB Lite — Local Emulator                 ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Endpoint:  https://localhost:%d/                        ║\n", *port)
	fmt.Println("║  Key:       C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2  ║")
	fmt.Println("║             nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==║")
	fmt.Printf("║  Explorer:  https://localhost:%d/_explorer/              ║\n", *port)
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Cert:      %s\n", certFile)
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Start server
	router := api.NewRouter(s, !*noAuth)
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting HTTPS server on %s", addr)
	if err := router.ListenAndServeTLS(addr, certFile, keyFile); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func defaultCertDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".cosmosdb-lite"
	}
	return filepath.Join(home, ".cosmosdb-lite")
}
