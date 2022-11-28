package server

import (
	"fmt"
	"sync"

	"pub-sub/domain"
)

type SubscriptionMgr struct {
	mutex sync.Mutex
	subs  map[string]([]*PeerConn)
}

func NewSubscriptionMgr() *SubscriptionMgr {
	return &SubscriptionMgr{
		mutex: sync.Mutex{},
		subs:  make(map[string]([]*PeerConn)),
	}
}

func mutex(mgr *SubscriptionMgr) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
}

func (mgr *SubscriptionMgr) Publish(msg *domain.Message) {
	mutex(mgr)

	hasError := false
	if peers, ok := mgr.subs[msg.Topic]; ok {
		for _, peer := range peers {
			err := peer.Write(msg)
			if err != nil {
				hasError = true
			}
		}
	}
	if hasError {
		mgr.flush()
	}
}

func (mgr *SubscriptionMgr) Subscribe(topic string, peer *PeerConn) {
	mutex(mgr)

	fmt.Printf("[Subscribe] %s =====> Peers: %s\n", topic, peer.conn.RemoteAddr())
	if candidates, ok := mgr.subs[topic]; ok {
		for _, oldPeer := range candidates {
			if oldPeer == peer {
				return
			}
		}
		mgr.subs[topic] = append(candidates, peer)
	} else {
		mgr.subs[topic] = []*PeerConn{peer}
	}
}

func (mgr *SubscriptionMgr) Unsubscribe(topic string, peer *PeerConn) {
	mutex(mgr)

	fmt.Printf("[Unsubscribe] %s =====> peer: %s\n", topic, peer.conn.RemoteAddr())
	if candidates, ok := mgr.subs[topic]; ok {
		newPeers := make([]*PeerConn, 0, len(candidates))
		for _, oldPeer := range candidates {
			if oldPeer != peer {
				newPeers = append(newPeers, oldPeer)
			}
		}
		if len(newPeers) == 0 {
			delete(mgr.subs, topic)
		} else {
			mgr.subs[topic] = newPeers
		}
	}
}

func (mgr *SubscriptionMgr) flush() {
	mutex(mgr)

	for topic, peers := range mgr.subs {
		newPeers := make([]*PeerConn, 0, len(peers))
		for _, peer := range peers {
			if !peer.IsClosed() {
				newPeers = append(newPeers, peer)
			}
		}
		mgr.subs[topic] = newPeers
	}
}
