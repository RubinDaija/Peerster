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
	Simple      *SimpleMessage
	Rumor       *RumorMessage
	Status      *StatusPacket
	Private     *PrivateMessage
	DataRequest *DataRequest
	DataReply   *DataReply
}

//Message - simple message struct
type Message struct {
	Text        string
	Destination *string
	File        *string
	Request     *[]byte
}

//PrivateMessage is the struct used to send private messages between two peers
type PrivateMessage struct {
	Origin      string
	ID          uint32
	Text        string
	Destination string
	HopLimit    uint32
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

//DataRequest struct
type DataRequest struct {
	Origin      string
	Destination string
	HopLimit    uint32
	HashValue   []byte
}

//DataReply struct
type DataReply struct {
	Origin      string
	Destination string
	HopLimit    uint32
	HashValue   []byte
	Data        []byte
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
	var resulting string
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
	if val == 0 && id == 1 { //add new origin to our map
		sp.status[origin] = 1
		sp.Unlock()
		return true
	}
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

//IPChanMap - this map will be used to have ip channel mappings so that  when we are waiting for a status message we know which IP we are looking for
type IPChanMap struct {
	sync.RWMutex
	ipChan map[string]chan int
}

//NewIPChanMap - initializing the IP channel map
func NewIPChanMap() *IPChanMap {
	channel := make(map[string]chan int)
	return &IPChanMap{ipChan: channel}
}

//DeleteIfExists - deletes the entry from the ip chan map if it exists there; returns true if it succedes hence the value exists
// This way even if we timeout delete the channel and then recieve the status and try to delete it again that won't be a problem
//this is also called by the process waiting if he timeouts as to remove the number of channels needed, he might write to the
//channel but that is not a problem as it will never read that value again
func (ipc *IPChanMap) DeleteIfExists(ip string) bool {
	ipc.Lock()
	value, existence := ipc.ipChan[ip]
	if existence {
		value <- 1 //this is intended to kill the process waiting
		delete(ipc.ipChan, ip)
	}
	ipc.Unlock()
	return existence
}

//AddEntry - adds ip to the map and creates a new channel for that ip
func (ipc *IPChanMap) AddEntry(ip string) *chan int {
	channelToUse := make(chan int, 2)
	ipc.Lock()
	ipc.ipChan[ip] = channelToUse
	ipc.Unlock()
	return &channelToUse
}

//GetChannel - returns the channel for a given IP
func (ipc *IPChanMap) GetChannel(ip string) *chan int {
	ipc.RLock()
	value, _ := ipc.ipChan[ip]
	ipc.RUnlock()
	return &value
}

//MsgMap - has all the messages with all origins
type MsgMap struct {
	sync.RWMutex
	messages map[string][]string
}

//NewMsgMap - initializing the msg map
func NewMsgMap() *MsgMap {
	variable := make(map[string][]string)
	return &MsgMap{messages: variable}
}

//AddMsg - add a message to the message map
func (mm *MsgMap) AddMsg(origin string, msg string, id uint32) bool {
	var status = false
	mm.Lock()
	value, _ := mm.messages[origin] //it returns an empty string array if it doesn't find anything so it still works
	if uint32(len(value)) == id-1 {
		mm.messages[origin] = append(value, msg)
		status = true
	}
	mm.Unlock()
	return status
}

//GetMsg - returns the message from the given origin and index; index has to be the id of the message
func (mm *MsgMap) GetMsg(origin string, ID uint32) string {
	index := ID - 1
	var toReturn string
	mm.RLock()
	value, _ := mm.messages[origin]
	toReturn = value[index]
	mm.RUnlock()
	return toReturn
}

//DSDVMap Will hold the origin IP:port combo
type DSDVMap struct {
	sync.RWMutex
	dsdv map[string]string
}

//NewDSDVMap - initializing the dsdv map
func NewDSDVMap() *DSDVMap {
	variable := make(map[string]string)
	return &DSDVMap{dsdv: variable}
}

//UpdateDSDV - shouldAdd parameter is passed as the result from update status
func (dsdv *DSDVMap) UpdateDSDV(origin string, ip string, shouldAdd bool) {
	if shouldAdd {
		dsdv.Lock()
		dsdv.dsdv[origin] = ip
		dsdv.Unlock()
	}
}

//GetIP - returns the IP that we should send the message concerning the current Origin given
func (dsdv *DSDVMap) GetIP(origin string) string {
	dsdv.RLock()
	val, _ := dsdv.dsdv[origin] //empty if there is no such IP, but this should not happen since if someone sends a message for us to route that means they got a message from that origin through us
	dsdv.RUnlock()
	return val
}

//ID struct since the rtimer will use it with the client
type ID struct {
	sync.Mutex
	id uint32
}

//InitializeID initializes the id
func InitializeID() *ID {
	return &ID{id: 0}
}

//IncID - increments the id by one and returns the result
func (id *ID) IncID() uint32 {
	id.Lock()
	val := id.id + 1
	id.id = val
	id.Unlock()
	return val
}
