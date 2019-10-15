package additional

import (
	"strings"
	"sync"
)

//SimpleMessage struct
type SimpleMessage struct {
	OriginalName  string // original name of the sending node
	RelayPeerAddr string //peers it has been relayed through ip:port
	Contents      string // the message
}

//GossipPacket struct
//Updated to include rumor message and status packets
type GossipPacket struct {
	Simple *SimpleMessage
	Rumor  *RumorMessage
	Status *StatusPacket
}

//Message - simple message struct
type Message struct {
	Text string
}

//RumorMessage struct - is the new type of message that will propagate through the network
type RumorMessage struct {
	Origin string
	ID     uint32
	Text   string
}

//PeerStatus struct - used to send status messages between peers
type PeerStatus struct {
	Identifier string
	NextID     uint32
}

//StatusPacket struct - will have several peerStatus structs inside each showing the acknowledged messages for that peer; it represents the status of the peer sending it
type StatusPacket struct {
	Want []PeerStatus
}

//PeersMap - normal map with read write mutex lock
type PeersMap struct {
	sync.RWMutex
	peers map[string]string
}

//NewPeersMap - Technically a constructor for the struct, we don't need a lock, runs at the very beginning
func NewPeersMap(peersIn string) *PeersMap {
	npeers := make(map[string]string)
	peersInSplit := strings.Split(peersIn, ",")
	if len(peersInSplit[0]) != 0 {

		for i := range peersInSplit {
			npeers[peersInSplit[i]] = peersInSplit[i]
		}
	}
	return &PeersMap{peers: npeers}
}

//LoadAll - returns all the peers in the map currently
func (pm *PeersMap) LoadAll() (resultPeers map[string]string) {
	pm.RLock()
	resultPeers = make(map[string]string) //we do this since we do not want a shallow copy
	for i := range pm.peers {
		resultPeers[i] = i
	}
	pm.RUnlock()
	return resultPeers
}

//StoreNotExists - stores a value in the map if it does not exist, we only need the key since key and value are the same thing
func (pm *PeersMap) StoreNotExists(key string) {
	pm.Lock()
	_, status := pm.peers[key] //We do this here not to waste time on getting the other lock and then getting this lock
	if !status {
		pm.peers[key] = key
	}
	pm.Unlock()
}

//ToString - string the peers as ip:port delimited by a coma
func (pm *PeersMap) ToString() string {
	var resulting string = ""
	pm.RLock()
	if len(pm.peers) >= 1 {
		for i := range pm.peers {
			resulting = resulting + i + ","
		}
	}
	pm.RUnlock()
	if len(resulting) == 0 {
		return ""
	}
	return resulting[:len(resulting)-1] //Remove the last coma
}

//StatusMap - a map that holds origins as keys and and int id as values
type StatusMap struct {
	sync.RWMutex
	status map[string]uint32
}

//NewStatusMap - initializing the status map
func NewStatusMap() *StatusMap {
	nstatus := make(map[string]uint32)
	return &StatusMap{status: nstatus}
}

//GetStatusMap the status map - used for when someone reads from the map
func (sp *StatusMap) GetStatusMap() map[string]uint32 {
	sp.RLock()
	newMap := make(map[string]uint32)
	for i := range sp.status {
		newMap[i] = sp.status[i]
	}
	sp.RUnlock()

	return newMap
}

//CreateStatusPacket - creates a status packet from the status map
func (sp *StatusMap) CreateStatusPacket() *StatusPacket {
	sp.RLock()
	tmpWant := make([]PeerStatus, len(sp.status))
	counter := 0
	for s := range sp.status {
		tmpWant[counter] = PeerStatus{Identifier: s, NextID: sp.status[s]}
		counter = counter + 1
	}
	sp.RUnlock()
	return &StatusPacket{Want: tmpWant}
}

//UpdateStatus - updates the status map only if we get the next message in line
//				 if the update is succesful it returns true, false otherwise
func (sp *StatusMap) UpdateStatus(origin string, id uint32) bool {
	sp.Lock()
	val := sp.status[origin]
	if val == id {
		sp.status[origin] = val + 1
		sp.Unlock()
		return true
	}
	sp.Unlock()
	return false

}

//InsertNewOrigin - inserts a new origin with id 1
func (sp *StatusMap) InsertNewOrigin(origin string) {
	sp.Lock()
	sp.status[origin] = 1
	sp.Unlock()
}

//SRResponsePacket only used when the status comparer returns a result to the main message receiver
type SRResponsePacket struct {
	IP        string
	Origin    string //set to empty if we do not need to send anything
	MessageID uint32
}

//SWResponse only used when getting a result from the function responsible for timeouting for messages sent
type SWResponse struct {
	IP     string
	Packet GossipPacket
}
