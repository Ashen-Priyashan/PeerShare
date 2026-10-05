package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Ashen-Priyashan/PeerShare/internal/peer"
	"github.com/Ashen-Priyashan/PeerShare/internal/transfer"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "peer:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", usage())
	}
	switch args[0] {
	case "server":
		return startServer(args[1:])
	case "client":
		return startClient(args[1:])
	case "monitor":
		return monitorPeers(args[1:])
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage())
	}
}

func usage() string {
	return "Usage: peer [server|client|monitor] [options]\n  server   Serve files from the shared directory\n  client   Discover peers and download a file\n  monitor  Periodically check discovered peers"
}

func startServer(args []string) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	listenAddress := flags.String("listen", ":5000", "TCP address to listen on")
	discoveryAddress := flags.String("discovery", ":5001", "UDP address for LAN discovery")
	sharedDirectory := flags.String("shared", "shared", "directory containing files to share")
	if err := flags.Parse(args); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("start TCP server: %w", err)
	}
	defer listener.Close()

	udpAddress, err := net.ResolveUDPAddr("udp4", *discoveryAddress)
	if err != nil {
		return fmt.Errorf("resolve discovery address: %w", err)
	}
	discoveryConn, err := net.ListenUDP("udp4", udpAddress)
	if err != nil {
		return fmt.Errorf("start peer discovery: %w", err)
	}
	defer discoveryConn.Close()

	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("read TCP listener address")
	}
	go func() {
		if err := peer.ServeDiscovery(discoveryConn, tcpAddress.Port); err != nil {
			fmt.Println("Peer discovery stopped:", err)
		}
	}()

	fmt.Println("PeerShare server listening on", listener.Addr())
	fmt.Println("Peer discovery listening on", discoveryConn.LocalAddr())
	fmt.Println("Waiting for peers to connect...")
	if err := peer.Serve(listener, *sharedDirectory, func(err error) {
		fmt.Println("Peer transfer failed:", err)
	}); err != nil {
		return fmt.Errorf("peer server stopped: %w", err)
	}
	return nil
}

func startClient(args []string) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	peerAddress := flags.String("peer", "", "server TCP address; discovers a peer when omitted")
	discoveryAddress := flags.String("discover", "255.255.255.255:5001", "UDP address to search for peers")
	discoveryTimeout := flags.Duration("discover-timeout", 2*time.Second, "time to wait for peer discovery responses")
	fileName := flags.String("file", "test.txt", "name of the file to request")
	outputPath := flags.String("output", "received_test.txt", "path for the received file")
	if err := flags.Parse(args); err != nil {
		return err
	}

	var peerAddresses []string
	if *peerAddress == "" {
		peers, err := peer.Discover(*discoveryAddress, *discoveryTimeout)
		if err != nil {
			return fmt.Errorf("peer discovery failed: %w", err)
		}
		if len(peers) == 0 {
			return fmt.Errorf("no peers found; specify a server with -peer host:port")
		}
		fmt.Println("Discovered peers:", strings.Join(peers, ", "))
		peerAddresses = peers
	} else {
		peerAddresses = []string{*peerAddress}
	}

	fmt.Println("Downloading from peers:", strings.Join(peerAddresses, ", "))
	stats, err := transfer.DownloadFromPeersWithStats(peerAddresses, *fileName, *outputPath)
	if err != nil {
		return fmt.Errorf("file transfer failed: %w", err)
	}
	fmt.Println("File received successfully:", *outputPath)
	fmt.Printf("Transfer: %d chunks, %d candidate peers, %.2f MiB in %s (%.2f MiB/s)\n",
		stats.ChunkCount,
		stats.PeerCount,
		float64(stats.SizeBytes)/(1<<20),
		stats.Duration.Round(time.Millisecond),
		stats.AverageBytesPerSecond/(1<<20),
	)
	return nil
}

func monitorPeers(args []string) error {
	flags := flag.NewFlagSet("monitor", flag.ContinueOnError)
	discoveryAddress := flags.String("discover", "255.255.255.255:5001", "UDP address used to discover peers")
	discoveryTimeout := flags.Duration("discover-timeout", 2*time.Second, "time to wait for discovery responses")
	probeTimeout := flags.Duration("probe-timeout", 2*time.Second, "maximum time to check a peer's TCP port")
	interval := flags.Duration("interval", 5*time.Second, "time between scans")
	once := flags.Bool("once", false, "perform one scan and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if !*once && *interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	fmt.Println("Monitoring peers; press Ctrl+C to stop.")
	for {
		results, err := peer.Scan(*discoveryAddress, *discoveryTimeout, *probeTimeout)
		if err != nil {
			return fmt.Errorf("peer scan failed: %w", err)
		}
		now := time.Now().Format(time.RFC3339)
		if len(results) == 0 {
			fmt.Printf("%s no peers discovered\n", now)
		}
		for _, result := range results {
			if result.Reachable {
				fmt.Printf("%s available %s (%s)\n", now, result.Address, result.Latency.Round(time.Millisecond))
			} else {
				fmt.Printf("%s unavailable %s: %s\n", now, result.Address, result.Error)
			}
		}
		if *once {
			return nil
		}
		timer := time.NewTimer(*interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
