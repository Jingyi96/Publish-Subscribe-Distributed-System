package server

import (
	"fmt"
	"net"
	"time"

	"pub-sub/domain"
)

type ServerStatus int

const (
	Election ServerStatus = iota
	Waiting
	Follower
	Leader
)

type Server struct {
	listener              net.Listener
	status                ServerStatus
	leaderAddr            string
	localAddr             string
	leaderCandidates      []string
	otherPeers            []string
	nextMsgId             int64
	subscriptionMgr       *SubscriptionMgr
	peerMgr               *PeerMgr
	lastElectionStartTime time.Time
	lastElectionOkTime    time.Time
	waitTimeout           time.Duration
	localMessageIngest    chan *domain.Message
	stashMessage          chan *domain.Message
	maxStashMessageCount  int
}

func NewServer(listenAddr string, peerAddrs []string) *Server {
	leaderCandidates := make([]string, 0)
	otherPeers := make([]string, 0)
	for _, addr := range peerAddrs {
		if addr == listenAddr {
			continue
		}
		if addr < listenAddr {
			leaderCandidates = append(leaderCandidates, addr)
		} else {
			otherPeers = append(otherPeers, addr)
		}
	}

	fmt.Printf("[LOG] Leader candidates: %v\nOthers: %v\n", leaderCandidates, otherPeers)

	socket, err := net.Listen("tcp", listenAddr)
	if err != nil {
		panic(err)
	}

	maxStashMessageCount := 1_00_00
	server := &Server{
		listener:              socket,
		status:                Election,
		leaderAddr:            "",
		localAddr:             listenAddr,
		leaderCandidates:      leaderCandidates,
		otherPeers:            otherPeers,
		nextMsgId:             0,
		subscriptionMgr:       NewSubscriptionMgr(),
		peerMgr:               NewPeerMgr(listenAddr),
		lastElectionStartTime: time.UnixMilli(0),
		lastElectionOkTime:    time.UnixMilli(0),
		waitTimeout:           time.Second * 3,
		localMessageIngest:    make(chan *domain.Message),
		stashMessage:          make(chan *domain.Message, maxStashMessageCount),
		maxStashMessageCount:  maxStashMessageCount,
	}
	go server.acceptConn()
	return server
}

func sep() {
	fmt.Println("*-*-*-*-=====================================-*-*-*-*")
}

func (s *Server) Init() {
	for {
		sep()
		fmt.Printf("Status ==> %s\tLeader ===> %s\n", s.status.Name(), s.leaderAddr)
		sep()

		switch s.status {
		case Election:
			s.status = s.election()
		case Waiting:
			s.status = s.waiting()
		case Follower:
			s.status = s.follower()
		case Leader:
			s.status = s.leader()
		}
	}
}

func (s *Server) acceptConn() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	pc := NewPeerFromConn(conn)
	msg := &domain.HandshakeMessage{}
	pc.Read(msg)

	switch msg.Type {
	case domain.HelloFromServerPeer:
		s.peerMgr.Put(msg.Addr, pc)
	case domain.HelloFromClient:
		s.handleClientRead(pc)
	}
}

func (s *Server) handleClientRead(cc *PeerConn) {
	for {
		msg := &domain.Message{}
		err := cc.Read(msg)

		if err != nil {
			break
		}
		fmt.Printf("message: %+v\n", msg)
		switch msg.Type {
		case domain.Normal:
			fmt.Printf("*** normal message: %v\n", msg)
			s.localMessageIngest <- msg
		case domain.Subscribe:
			s.subscriptionMgr.Subscribe(msg.Topic, cc)
		case domain.Unsubscribe:
			s.subscriptionMgr.Unsubscribe(msg.Topic, cc)
		default:
			fmt.Printf("[ERROR] Unkown message format: %v\n", msg)
		}
	}
}

func (s *Server) stash(msg *domain.Message) {
	select {
	case s.stashMessage <- msg:
	default:
		// Stash fulled, do nothing
	}
}

func (s *Server) waiting() ServerStatus {
	select {
	// if timeout, back to election status
	case <-time.After(s.waitTimeout):
		return Election
	case msg := <-s.peerMgr.GetCh():
		switch msg.Type {
		case domain.Election:
			s.sendElectionOk(msg.SrcAddr)
		case domain.ElectionOK:
			fmt.Printf("[LOG] Finished election from: %v\n", msg.SrcAddr)
			s.lastElectionOkTime = time.Now()
		case domain.Coordinator:
			s.leaderAddr = msg.SrcAddr
			return Follower
		case domain.ReadClosed:
			// ignore
		default:
			s.stash(msg)
		}
	}
	return Waiting
}

func (s *Server) becomeLeader() {
	s.leaderAddr = s.localAddr
	msg := &domain.Message{
		Type:    domain.Coordinator,
		SrcAddr: s.localAddr,
	}
	for _, peerAddr := range s.otherPeers {
		peerConn, err := s.peerMgr.Get(peerAddr)
		if err != nil {
			continue
		}
		err = peerConn.Write(msg)
		if err != nil {
			fmt.Printf("[ERROR] Send coordinator to peer: %v failed %v\n", peerAddr, err)
		}
	}
}

func (s *Server) election() ServerStatus {
	if s.lastElectionOkTime.Before(s.lastElectionStartTime) {
		s.becomeLeader()
		return Leader
	}
	s.lastElectionStartTime = time.Now()
	sendElectionSuccessCount := 0
	for _, peerAddr := range s.leaderCandidates {
		peerConn, err := s.peerMgr.Get(peerAddr)
		if err != nil {
			continue
		}
		msg := &domain.Message{
			Type:    domain.Election,
			SrcAddr: s.localAddr,
		}
		err = peerConn.Write(msg)
		if err != nil {
			continue
		}
		fmt.Printf("send election to %v\n", peerAddr)
		sendElectionSuccessCount += 1
	}
	if sendElectionSuccessCount == 0 {
		s.becomeLeader()
		return Leader
	}
	return Waiting
}

func (s *Server) sendMsgToLeader(msg *domain.Message) ServerStatus {
	if s.leaderAddr == s.localAddr {
		panic("send msg to leader when leader")
	}
	leaderConn, err := s.peerMgr.Get(s.leaderAddr)
	if err != nil {
		return Election
	}
	err = leaderConn.Write(msg)
	if err != nil {
		return Election
	}
	return Follower
}

func (s *Server) sendElectionOk(addr string) error {
	if addr == s.localAddr {
		panic("[WARNING] Election to myself")
	}
	msg := &domain.Message{
		Type:    domain.ElectionOK,
		SrcAddr: s.localAddr,
	}
	peerConn, err := s.peerMgr.Get(addr)
	if err != nil {
		return err
	}
	err = peerConn.Write(msg)
	if err == nil {
		fmt.Printf("[LOG] Send election ok to %v\n", addr)
	}
	return err
}

func (s *Server) follower() ServerStatus {
	select {
	case msg := <-s.stashMessage:
		switch msg.Type {
		case domain.Normal:
			return s.sendMsgToLeader(msg)
		default:
			fmt.Printf("[ERROR] Slave: Unexpected message from the stash: %v\n", msg)
		}

	case msg := <-s.localMessageIngest:
		return s.sendMsgToLeader(msg)

	case msg := <-s.peerMgr.GetCh():
		switch msg.Type {
		case domain.Election:
			s.sendElectionOk(msg.SrcAddr)
			return Election
		case domain.Coordinator:
			s.leaderAddr = msg.SrcAddr
		case domain.Normal:
			s.nextMsgId = msg.Id + 1
			s.subscriptionMgr.Publish(msg)
		case domain.ReadClosed:
			if msg.SrcAddr == s.leaderAddr {
				return Election
			}
		default:
			fmt.Printf("[ERROR] Slave: Unexpected message received from channel: %v\n", msg)
		}
	}
	return Follower
}

func (s *Server) leader() ServerStatus {
	select {
	case msg := <-s.stashMessage:
		switch msg.Type {
		case domain.Normal:
			s.updateMessageId(msg)
			s.peerMgr.Boardcast(msg)
			s.subscriptionMgr.Publish(msg)
		default:
			fmt.Printf("[ERROR] Leader: unexpected message from stash: %v\n", msg)
		}
	case msg := <-s.localMessageIngest:
		s.updateMessageId(msg)
		s.peerMgr.Boardcast(msg)
		s.subscriptionMgr.Publish(msg)

	case msg := <-s.peerMgr.GetCh():
		switch msg.Type {
		case domain.Election:
			s.sendElectionOk(msg.SrcAddr)
			return Election
		case domain.Coordinator:
			s.leaderAddr = msg.SrcAddr
			return Follower
		case domain.Normal:
			s.updateMessageId(msg)
			s.peerMgr.Boardcast(msg)
			s.subscriptionMgr.Publish(msg)
		case domain.ReadClosed:
			// ignore
		default:
			fmt.Printf("[ERROR] Leader: unexpected message received from channel: %v\n", msg)
		}
	}
	return Leader
}

func (status ServerStatus) Name() string {
	switch status {
	case Election:
		return "Election"
	case Waiting:
		return "Waiting"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

func (s *Server) updateMessageId(msg *domain.Message) {
	msg.Id = s.nextMsgId
	s.nextMsgId++
}
