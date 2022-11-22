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

func (s ServerStatus) Name() string {
	switch s {
	case Election:
		return "Election"
	case Waiting:
		return "Waiting"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

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
	lastElectionOKTime    time.Time
	waitTimeout           time.Duration
	localMessageIngest    chan *domain.Message
	stashMessage          chan *domain.Message
	maxStashMessageCount  int
}

func NewServer(listenerAddr string, peerAddrs []string) *Server {
	leaderCandidates := make([]string, 0)
	otherPeers := make([]string, 0)

	for _, addr := range peerAddrs {
		if addr == listenerAddr {
			continue
		}

		if addr < listenerAddr {
			leaderCandidates = append(leaderCandidates, addr)
		} else {
			otherPeers = append(otherPeers, addr)
		}
	}

	fmt.Printf("Leader Candidates: %v\n", leaderCandidates)
	fmt.Printf("Other Candidates: %v\n", otherPeers)

	ln, err := net.Listen("tcp", listenerAddr)
	if err != nil {
		panic(err)
	}

	maxStashMessageCount := 100 * 100

	server := &Server{
		listener:              ln,
		status:                Election,
		leaderAddr:            "",
		localAddr:             listenerAddr,
		otherPeers:            otherPeers,
		nextMsgId:             0,
		subscriptionMgr:       NewSubscriptionMgr(),
		peerMgr:               NewPeerMgr(listenerAddr),
		lastElectionStartTime: time.UnixMilli(0),
		lastElectionOKTime:    time.UnixMilli(0),
		waitTimeout:           time.Second * 2,
		localMessageIngest:    make(chan *domain.Message),
		stashMessage:          make(chan *domain.Message, maxStashMessageCount),
		maxStashMessageCount:  maxStashMessageCount,
	}

	go server.acceptConn()
	return server
}

func (s *Server) init() {
	for {
		fmt.Printf("[%v] - [%s]: leader is %s\n", s.localAddr, s.status.Name(), s.leaderAddr)
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
			fmt.Printf("Error! Accept Connection")
			fmt.Println(err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	peerConn := NewPeerFromConn(conn)
	initMsg := &domain.HandshakeMessage{}
	peerConn.Read(initMsg)

	switch initMsg.Type {
	case domain.HelloFromClient:
		s.handleClientRead(peerConn)
	case domain.HelloFromServerPeer:
		s.peerMgr.Put(initMsg.Addr, peerConn)
	}
}

func (s *Server) handleClientRead(cc *PeerConn) {
	for {
		msg := &domain.Message{}
		err := cc.Read(msg)

		if err != nil {
			fmt.Printf("There is an error while decoding the message %v\n", err)
			break
		}

		fmt.Printf("The message is %+v\n", msg)

		switch msg.Type {
		case domain.Normal:
			fmt.Printf("[Normal]: %v\n", msg)
			s.localMessageIngest <- msg
		case domain.Subscribe:
			s.subscriptionMgr.Subscribe(msg.Topic, cc)
		case domain.Unsubscribe:
			s.subscriptionMgr.Unsubscribe(msg.Topic, cc)
		default:
			fmt.Printf("Unsupported Message: %v\n", msg)
		}
	}
}

func (s *Server) stash(msg *domain.Message) {
	select {
	case s.stashMessage <- msg:
	default:
		fmt.Printf("Stash messages are fulled, max message count %v, drop %v\n", s.maxStashMessageCount, msg)
	}
}

func (s *Server) waiting() ServerStatus {
	select {
	case <-time.After(s.waitTimeout):
		return Election
	case msg := <-s.peerMgr.GetCh():
		switch msg.Type {
		case domain.Election:
			s.sendElectionOK(msg.SrcAddr)
		case domain.ElectionOK:
			fmt.Printf("Election OK: %v\n", msg.SrcAddr)
			s.lastElectionOKTime = time.Now()
		case domain.Coordinator:
			s.leaderAddr = msg.SrcAddr
			return Follower
		case domain.ReadClosed:
			// Do nothing
		default:
			s.stash(msg)
		}
	}
	return Waiting
}

func (s *Server) sendElectionOK(addr string) error {
	if addr == s.localAddr {
		panic("Send election OK to itself")
	}

	msg := &domain.Message{
		Type:    domain.ElectionOK,
		SrcAddr: s.localAddr,
	}

	pc, err := s.peerMgr.Get(addr)
	if err != nil {
		return err
	}

	err = pc.Write(msg)
	if err == nil {
		fmt.Printf("Send election ok to %v\n", addr)
	}
	return err
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
			fmt.Printf("Send coordinator to peer: %v, failed %v\n", peerAddr, err)
		}
	}
}

func (s *Server) election() ServerStatus {
	if s.lastElectionOKTime.Before(s.lastElectionOKTime) {
		s.becomeLeader()
		return Leader
	}

	s.lastElectionStartTime = time.Now()
	electionSuccessCount := 0

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

		fmt.Printf("Send election to %v\n", peerAddr)
		electionSuccessCount++
	}

	if electionSuccessCount == 0 {
		s.becomeLeader()
		return Leader
	}
	return Waiting
}

func (s *Server) sendMsgToLeader(msg *domain.Message) ServerStatus {
	if s.leaderAddr == s.localAddr {
		panic("Send message to leader")
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

func (s *Server) follower() ServerStatus {
	select {
	case msg := <-s.stashMessage:
		switch msg.Type {
		case domain.Normal:
			return s.sendMsgToLeader(msg)
		default:
			fmt.Printf("[Follower] Receieved unexpected message from %v\n", msg)
		}
	case msg := <-s.localMessageIngest:
		return s.sendMsgToLeader(msg)
	case msg := <-s.peerMgr.GetCh():
		switch msg.Type {
		case domain.Election:
			s.sendElectionOK(msg.SrcAddr)
			return Election
		case domain.Coordinator:
			s.leaderAddr = msg.SrcAddr
			return Follower
		case domain.Normal:
			s.nextMsgId = msg.Id + 1
			s.subscriptionMgr.Publish(msg)
		case domain.ReadClosed:
			if msg.SrcAddr == s.leaderAddr {
				return Election
			}
		default:
			fmt.Printf("Follower: Unexpected message from peer channel: %v\n", msg)
		}
	}
	return Follower
}

func (s *Server) leader() ServerStatus {
	select {
	case msg := <-s.stashMessage:
		switch msg.Type {
		case domain.Normal:
			s.assignMsgId(msg)
			s.peerMgr.Boardcast(msg)
			s.subscriptionMgr.Publish(msg)
		default:
			fmt.Printf("Leader: unexpected msg from stashed message: %v\n", msg)
		}
	case msg := <-s.localMessageIngest:
		s.assignMsgId(msg)
		s.peerMgr.Boardcast(msg)
		s.subscriptionMgr.Publish(msg)

	case msg := <-s.peerMgr.GetCh():
		switch msg.Type {
		case domain.Election:
			s.sendElectionOK(msg.SrcAddr)
			return Election
		case domain.Coordinator:
			s.leaderAddr = msg.SrcAddr
			return Follower
		case domain.Normal:
			s.assignMsgId(msg)
			s.peerMgr.Boardcast(msg)
			s.subscriptionMgr.Publish(msg)
		case domain.ReadClosed:
			// ignore
		default:
			fmt.Printf("Leader: unexpected msg from peer chan: %v\n", msg)
		}
	}
	return Leader
}

func (s *Server) assignMsgId(msg *domain.Message) {
	msg.Id = s.nextMsgId
	s.nextMsgId++
}
