package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourusername/kvstore/internal/engine"
	"github.com/yourusername/kvstore/internal/server"
)

func main() {
	port := flag.String("port", "6380", "TCP port to listen on")
	dataDir := flag.String("data-dir", "./data", "data directory for SSTables and WAL")
	memtabMB := flag.Int64("memtable-mb", 4, "MemTable size in MB before flush")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[kvstore] ")

	eng, err := engine.Open(engine.Config{
		DataDir:      *dataDir,
		MemTableSize: *memtabMB * 1024 * 1024,
		WALSync:      true,
	})
	if err != nil {
		log.Fatalf("engine open: %v", err)
	}
	defer func() {
		log.Println("shutting down engine...")
		eng.Close()
	}()

	srv := server.NewServer(server.Config{
		Addr:       ":" + *port,
		MaxClients: 200,
	}, eng)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("received shutdown signal")
		srv.Shutdown()
	}()

	log.Printf("ConcurrentKV server starting on :%s (data=%s, memtable=%dMB)",
		*port, *dataDir, *memtabMB)

	if err := srv.ListenAndServe(); err != nil {
		log.Printf("server stopped: %v", err)
	}
}
