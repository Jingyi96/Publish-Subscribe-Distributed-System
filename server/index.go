package server

func Run(addr string, peers []string) {
	println("Server is initiating ...")
	server := NewServer(addr, peers)
	server.init()
}
