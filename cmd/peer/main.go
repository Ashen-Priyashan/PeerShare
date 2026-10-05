package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Ashen-Priyashan/PeerShare/internal/peer"
	"github.com/Ashen-Priyashan/PeerShare/internal/transfer"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: peer [server|client] [options]")
		return
	}

	switch os.Args[1] {
	case "server":
		startServer(os.Args[2:])
	case "client":
		startClient(os.Args[2:])
	default:
		fmt.Println("Unknown mode. Use server or client.")
	}
}

func startServer(args []string) {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	listenAddress := flags.String("listen", ":5000", "TCP address to listen on")
	discoveryAddress := flags.String("discovery", ":5001", "UDP address for LAN discovery")
	if err := flags.Parse(args); err != nil {
		return
	}

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	udpAddress, err := net.ResolveUDPAddr("udp4", *discoveryAddress)
	if err != nil {
		fmt.Println("Invalid discovery address:", err)
		return
	}
	discoveryConn, err := net.ListenUDP("udp4", udpAddress)
	if err != nil {
		fmt.Println("Error starting peer discovery:", err)
		return
	}
	defer discoveryConn.Close()

	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		fmt.Println("Error reading TCP listener address")
		return
	}
	go func() {
		if err := peer.ServeDiscovery(discoveryConn, tcpAddress.Port); err != nil {
			fmt.Println("Peer discovery stopped:", err)
		}
	}()

	fmt.Println("PeerShare server listening on", listener.Addr())
	fmt.Println("Peer discovery listening on", discoveryConn.LocalAddr())
	fmt.Println("Waiting for peers to connect...")
	if err := peer.Serve(listener, "shared", func(err error) {
		fmt.Println("Peer transfer failed:", err)
	}); err != nil {
		fmt.Println("Peer server stopped:", err)
	}
}

func startClient(args []string) {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	peerAddress := flags.String("peer", "", "server TCP address; discovers a peer when omitted")
	discoveryAddress := flags.String("discover", "255.255.255.255:5001", "UDP address to search for peers")
	discoveryTimeout := flags.Duration("discover-timeout", 2*time.Second, "time to wait for peer discovery responses")
	fileName := flags.String("file", "test.txt", "name of the file to request")
	outputPath := flags.String("output", "received_test.txt", "path for the received file")
	if err := flags.Parse(args); err != nil {
		return
	}

	var peerAddresses []string
	if *peerAddress == "" {
		peers, err := peer.Discover(*discoveryAddress, *discoveryTimeout)
		if err != nil {
			fmt.Println("Peer discovery failed:", err)
			return
		}
		if len(peers) == 0 {
			fmt.Println("No peers found. Specify a server with -peer host:port.")
			return
		}
		fmt.Println("Discovered peers:", strings.Join(peers, ", "))
		peerAddresses = peers
	} else {
		peerAddresses = []string{*peerAddress}
	}

	fmt.Println("Downloading from peers:", strings.Join(peerAddresses, ", "))
	stats, err := transfer.DownloadFromPeersWithStats(peerAddresses, *fileName, *outputPath)
	if err != nil {
		fmt.Println("File transfer failed:", err)
		return
	}
	fmt.Println("File received successfully:", *outputPath)
	fmt.Printf("Transfer: %d chunks, %d candidate peers, %.2f MiB in %s (%.2f MiB/s)\n",
		stats.ChunkCount,
		stats.PeerCount,
		float64(stats.SizeBytes)/(1<<20),
		stats.Duration.Round(time.Millisecond),
		stats.AverageBytesPerSecond/(1<<20),
	)
}
