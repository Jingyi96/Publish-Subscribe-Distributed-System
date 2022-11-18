package main

import (
	f "flag"
	"fmt"
)

var (
	mode       = f.String("mode", "server", "Please specify either <client> or <server>")
	serverInfo = f.String("server-addr", "127.0.0.1:5001", "server's address with port number, specifically")
	clientInfo = f.String("peer-addrs", "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003", "client's address with port number, using comma to seperate, no space")
)

func main() {
	f.Parse()

	switch *mode {
	case "server":
		fmt.Println("This is server")
	case "client":
		fmt.Println("This is client")
	}
}
