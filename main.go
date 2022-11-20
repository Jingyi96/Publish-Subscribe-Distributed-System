package main

import (
	f "flag"
	"fmt"
	s "strings"

	"pub-sub/domain"
	"pub-sub/server"
)

var (
	mode       = f.String("mode", "server", "Please specify either <client> or <server>")
	serverInfo = f.String("server-addr", "127.0.0.1:5001", "server's address with port number, specifically")
	clientInfo = f.String("peer-addrs", "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003", "client's address with port number, using comma to seperate, no space")
)

func main() {
	f.Parse()
	domain.Register()

	switch *mode {
	case "server":
		server.Run(*serverInfo, s.Split(*clientInfo, ","))
	case "client":
		fmt.Println("This is client")
	}
}
