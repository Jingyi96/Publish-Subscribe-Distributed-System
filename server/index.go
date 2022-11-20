package server

import "fmt"

func Run(addr string, peers []string) {
	println("Server is initiating ...")
	fmt.Printf("The server address is %s\nThe peers is %v\n", addr, peers)
}
