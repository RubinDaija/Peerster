package main

//Gossiper
import (
	"Peerster/additional"
	"Peerster/protobuf"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

//definig the timeout for a message sent
var waitingTimeout = 10

//function checks if a peer exists in our peers list, if he doesn't then it add it
func addPeer(peers *additional.PeersMap, newPeer string) {
	peers.StoreNotExists(newPeer)
}

func sendMsgToAllPeers(peersMap *additional.PeersMap, msg additional.GossipPacket) {
	peers := peersMap.LoadAll()
	for peer := range peers {
		sendMessageToPeer(peer, msg)
	}
}

func sendMsgToAllButOne(peersMap *additional.PeersMap, msg additional.GossipPacket, exception string) {
	peers := peersMap.LoadAll()
	for peer := range peers {
		if strings.Compare(peer, exception) != 0 { //We don't want to resend the message to the peer that sent it to us
			sendMessageToPeer(peer, msg)
		}
	}
}

//this method listens to the messages from peers and then broadcasts that message to all peers but the one who sent the message
func listenToPeersSimple(peers *additional.PeersMap, gossiperAddr string) {
	//setup the connection
	addr, err := net.ResolveUDPAddr("udp4", gossiperAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	connection, err := net.ListenUDP("udp4", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer connection.Close() //close the connection in the end
	buffer := make([]byte, 1024)

	gossipPack := &additional.GossipPacket{}

	for { //listens forever
		connection.ReadFromUDP(buffer)      //n, addr, err
		protobuf.Decode(buffer, gossipPack) //decode the message
		message := gossipPack.Simple
		sendingPeerAddr := message.RelayPeerAddr
		peers.StoreNotExists(sendingPeerAddr)          //store the new peer to known peers
		gossipPack.Simple.RelayPeerAddr = gossiperAddr //change relay peer address
		fmt.Println("SIMPLE MESSAGE origin", message.OriginalName, "from", sendingPeerAddr, "contents", message.Contents)
		fmt.Println("PEERS", peers.ToString())
		go sendMsgToAllButOne(peers, *gossipPack, sendingPeerAddr) //send message to all the other peers

	}
}

//this method listens to a client message and then broadcasts that message to all peers
func listenToClientSimple(peers *additional.PeersMap, PORT string, gossiperAddr string, nodeName string) {
	//setup the connection
	addr, err := net.ResolveUDPAddr("udp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}

	connection, err := net.ListenUDP("udp4", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer connection.Close() //close the connection in the end
	buffer := make([]byte, 1024)

	message := &additional.Message{}

	for { //listens forever
		connection.ReadFromUDP(buffer)   //n, addr, err
		protobuf.Decode(buffer, message) //decode the message
		fmt.Println("CLIENT MESSAGE", message.Text)
		// fmt.Println("PEERS ", peers.ToString()) //Do not print the peers when the client receives a message Apperently
		gossipPack := &additional.GossipPacket{Simple: &additional.SimpleMessage{OriginalName: nodeName, RelayPeerAddr: gossiperAddr, Contents: message.Text}, Rumor: nil, Status: nil, Private: nil}
		gossipPack.Simple.RelayPeerAddr = gossiperAddr
		gossipPack.Simple.OriginalName = nodeName
		go sendMsgToAllPeers(peers, *gossipPack) //create a new thread so that this thread starts to listen again
	}
}

//This function sends the message to the peer given
func sendMessageToPeer(peer string, msg additional.GossipPacket) {
	addr, err := net.ResolveUDPAddr("udp4", peer)
	if err != nil {
		fmt.Println(err)
		return
	}
	connection, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		fmt.Println(err)
		return
	}
	packetBytes, err := protobuf.Encode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = connection.Write(packetBytes)
	if err != nil {
		fmt.Println(err)
		return
	}
	connection.Close()
}

//This function sends the message to the peer given, using as the given port the one that we provide
func sendMsgToPeerExplicit(conn *net.UDPConn, peer string, msg additional.GossipPacket) {
	addr, err := net.ResolveUDPAddr("udp4", peer)
	if err != nil {
		fmt.Println(err)
		return
	}
	packetBytes, err := protobuf.Encode(&msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = conn.WriteToUDP(packetBytes, addr)
	if err != nil {
		fmt.Println(err)
		return
	}
}

//send and wait for message to peer - if it timeouts it sends it to someone else randlomly otherwise it dies
func swMsgPeer(ipchan *additional.IPChanMap, recv *chan int, peers *additional.PeersMap, conn *net.UDPConn, packet additional.GossipPacket, peer string, timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	select {
	case <-ticker.C:
		close(*recv)
		ipchan.DeleteIfExists(peer)
		sendRandomPeer(peers, conn, ipchan, packet)
		return
	case <-*recv: //all recv channels are unique to one thread
		close(*recv)
		return
	}

}

//compares our status packet with their and if we have smth we send it otherwise it puts nil in the origin field of SRResponsePacket to signify that we should send a status packet
func compareStatus(ourStatus *additional.StatusMap, theirStatus additional.StatusPacket, conn *net.UDPConn, peer string, ipchan *additional.IPChanMap, allmsg *additional.MsgMap, peers *additional.PeersMap, latestpacket additional.GossipPacket) {
	ourMap := ourStatus.GetStatusMap()
	theirMap := make(map[string]uint32)
	//convert their status packet to a map
	statusArr := theirStatus.Want
	for _, stat := range statusArr {
		theirMap[stat.Identifier] = stat.NextID
		//at the same time do the comparison them with us
		ourID, existence := ourMap[stat.Identifier]
		if !existence { //if we dont have it we simply send a status packet
			packet := additional.GossipPacket{Simple: nil, Rumor: nil, Status: ourStatus.CreateStatusPacket(), Private: nil}
			sendMsgToPeerExplicit(conn, peer, packet)
			return
		} else if ourID < stat.NextID { //we don't have the most up to date version so we send a status packet
			packet := additional.GossipPacket{Simple: nil, Rumor: nil, Status: ourStatus.CreateStatusPacket(), Private: nil}
			sendMsgToPeerExplicit(conn, peer, packet)
			return
		} else if stat.NextID < ourID { //we have the most up to date version
			content := allmsg.GetMsg(stat.Identifier, stat.NextID)
			packet := additional.GossipPacket{Simple: nil, Rumor: &additional.RumorMessage{Origin: stat.Identifier, ID: stat.NextID, Text: content}, Private: nil}
			channel := ipchan.AddEntry(peer)
			go swMsgPeer(ipchan, channel, peers, conn, packet, peer, waitingTimeout)
			sendMsgToPeerExplicit(conn, peer, packet)
			fmt.Println("MONGERING with", peer)
			return
		}
	}

	for identifier, ourID := range ourMap {
		theirID, existence := theirMap[identifier]
		if !existence {
			content := allmsg.GetMsg(identifier, 1) //they do not have these set of messages so they need to go in order
			packet := additional.GossipPacket{Simple: nil, Rumor: &additional.RumorMessage{Origin: identifier, ID: 1, Text: content}, Private: nil}
			channel := ipchan.AddEntry(peer)
			go swMsgPeer(ipchan, channel, peers, conn, packet, peer, waitingTimeout)
			sendMsgToPeerExplicit(conn, peer, packet)
			fmt.Println("MONGERING with", peer)
			return
		} else if ourID < theirID {
			packet := additional.GossipPacket{Simple: nil, Rumor: nil, Status: ourStatus.CreateStatusPacket(), Private: nil}
			sendMsgToPeerExplicit(conn, peer, packet)
			return
		} else if theirID < ourID { //we have the most up to date version
			content := allmsg.GetMsg(identifier, theirID)
			packet := additional.GossipPacket{Simple: nil, Rumor: &additional.RumorMessage{Origin: identifier, ID: theirID, Text: content}, Private: nil}
			channel := ipchan.AddEntry(peer)
			go swMsgPeer(ipchan, channel, peers, conn, packet, peer, waitingTimeout)
			sendMsgToPeerExplicit(conn, peer, packet)
			fmt.Println("MONGERING with", peer)
			return
		}
	}

	//if none of the above occur then we have the same set of messages and as a result we do the coin flip
	fmt.Println("IN SYNC WITH", peer)
	seed := rand.NewSource(time.Now().UnixNano()) //seeding
	rng := rand.New(seed)
	coin := rng.Int() % 2
	if coin == 0 { //head we rummor monger
		//start random rummor mongering
		randomPeer := sendRandomPeer(peers, conn, ipchan, latestpacket)
		if len(randomPeer) > 0 {
			fmt.Println("FLIPPED COIN sending rumor to", randomPeer)
		}
	} //tails we dont do anything
}

//the antiEntropy function will be run immediately on start up. TODO check if time should be seconds or milliseconds
func antiEntropy(status *additional.StatusMap, peers *additional.PeersMap, connection *net.UDPConn, timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	select {
	case <-ticker.C:
		peerMp := peers.LoadAll()
		if len(peerMp) > 0 { //we only can start if we have peers
			//choose random peer
			seed := rand.NewSource(time.Now().UnixNano()) //seeding
			rng := rand.New(seed)
			randomNum := rng.Intn(len(peerMp))
			counter := 0
			choosenPeer := ""
			for i := range peerMp {
				if counter == randomNum {
					choosenPeer = i
					break
				}
				counter = counter + 1
			}
			packet := additional.GossipPacket{Simple: nil, Rumor: nil, Status: status.CreateStatusPacket(), Private: nil}
			//send the peer choosen
			sendMsgToPeerExplicit(connection, choosenPeer, packet)
		}

	}
}

//this function sends a route rummor every rtime seconds
func contRouteRummor(peers *additional.PeersMap, connection *net.UDPConn, rtimer int, id *additional.ID, ourName string) {
	if rtimer > 0 {
		//bcast route rummor first time to all peers
		msgID := id.IncID()
		rumorPacket := &additional.RumorMessage{Origin: ourName, ID: msgID, Text: ""}
		packet := additional.GossipPacket{Simple: nil, Rumor: rumorPacket, Status: nil, Private: nil}
		sendMsgToAllPeers(peers, packet)

		ticker := time.NewTicker(time.Duration(rtimer) * time.Second)
		select {
		case <-ticker.C:
			peerMp := peers.LoadAll()
			if len(peerMp) > 0 { //we only can start if we have peers
				//choose random peer
				seed := rand.NewSource(time.Now().UnixNano()) //seeding
				rng := rand.New(seed)
				randomNum := rng.Intn(len(peerMp))
				counter := 0
				choosenPeer := ""
				for i := range peerMp {
					if counter == randomNum {
						choosenPeer = i
						break
					}
					counter = counter + 1
				}
				msgID = id.IncID()
				rumorPacket = &additional.RumorMessage{Origin: ourName, ID: msgID, Text: ""}
				packet = additional.GossipPacket{Simple: nil, Rumor: rumorPacket, Status: nil, Private: nil}
				//send the peer choosen
				sendMsgToPeerExplicit(connection, choosenPeer, packet)
			}

		}
	}
}

//listening to peer messages continuosly
func listenToMessages(conn *net.UDPConn, peers *additional.PeersMap, msgs *additional.MsgMap, status *additional.StatusMap, ipchan *additional.IPChanMap,
	dsdv *additional.DSDVMap, pmsg *additional.PrivateMsgMap, ourName string, files *additional.Files) {
	buffer := make([]byte, 1024)
	var latestRumor additional.GossipPacket
	for {
		gossipPack := &additional.GossipPacket{}
		_, recvFrom, _ := conn.ReadFromUDP(buffer) //n, addr, err
		protobuf.Decode(buffer, gossipPack)        //decode the message

		//check if we know the peer
		peers.StoreNotExists(recvFrom.String())

		peersString := peers.ToString()

		//send a status packet
		go sendMsgToPeerExplicit(conn, recvFrom.String(), additional.GossipPacket{Simple: nil, Rumor: nil, Status: status.CreateStatusPacket(), Private: nil})

		//Check the type of the packet
		if gossipPack.Rumor != nil {
			latestRumor = *gossipPack
			//print the rummor message and peers
			fmt.Println("RUMOR origin", gossipPack.Rumor.Origin, "from", recvFrom.String(), "ID", gossipPack.Rumor.ID, "contents", gossipPack.Rumor.Text)
			fmt.Println("PEERS", peersString)

			//store message
			msgs.AddMsg(gossipPack.Rumor.Origin, gossipPack.Rumor.Text, gossipPack.Rumor.ID)
			//update the status of our node
			updateStatusRes := status.UpdateStatus(gossipPack.Rumor.Origin, gossipPack.Rumor.ID)

			//update dsdv table if necessary
			go dsdv.UpdateDSDV(gossipPack.Rumor.Origin, recvFrom.String(), updateStatusRes)
			if len(gossipPack.Rumor.Origin) > 0 && updateStatusRes {
				fmt.Println("DSDV", gossipPack.Rumor.Origin, recvFrom.String())
			}

			//start random rumor mongering
			go sendRandomPeer(peers, conn, ipchan, *gossipPack)

		} else if gossipPack.Status != nil {
			//print status
			go printStatus(*gossipPack.Status, recvFrom.String(), peers)
			//compare statuses
			go compareStatus(status, *gossipPack.Status, conn, recvFrom.String(), ipchan, msgs, peers, latestRumor)
			//we also check if it is a response to one of our messages or just anti-entropy or the message has timeouted
			ipchan.DeleteIfExists(recvFrom.String())

		} else if gossipPack.Private != nil {
			privatePacket := *gossipPack.Private
			if privatePacket.Destination == ourName {
				pmsg.AddMsg(ourName, privatePacket.Text)
				fmt.Println("PRIVATE origin", privatePacket.Origin, "hop-limit", privatePacket.HopLimit, "contents", privatePacket.Text)
			} else {
				if privatePacket.HopLimit > 0 {
					privatePacket.HopLimit = privatePacket.HopLimit - 1
					ipToSend := dsdv.GetIP(privatePacket.Destination)
					packet := additional.GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: &privatePacket}
					sendMsgToPeerExplicit(conn, ipToSend, packet)
				}
			}

		} else if gossipPack.DataReply != nil {
			dreply := *gossipPack.DataReply
			if dreply.Destination == ourName {
				go files.StoreNewChunk(dreply, conn, dsdv)
			} else {
				if dreply.HopLimit > 0 {
					dreply.HopLimit = dreply.HopLimit - 1
					ipToSend := dsdv.GetIP(dreply.Destination)
					packet := additional.GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: nil, DataReply: &dreply}
					sendMsgToPeerExplicit(conn, ipToSend, packet)
				}
			}

		} else if gossipPack.DataRequest != nil {
			dreq := *gossipPack.DataRequest
			if dreq.Destination == ourName {
				go files.CheckNReturnChunk(dreq, dsdv, conn)
			} else {
				if dreq.HopLimit > 0 {
					dreq.HopLimit = dreq.HopLimit - 1
					ipToSend := dsdv.GetIP(dreq.Destination)
					packet := additional.GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: &dreq, DataReply: nil}
					sendMsgToPeerExplicit(conn, ipToSend, packet)
				}
			}

		} else { //TODO handle simple messages

		}

	}
}

//this function starts everything practically
func bootstrap(peers *additional.PeersMap, status *additional.StatusMap, msgs *additional.MsgMap, ipchan *additional.IPChanMap,
	gossiperAddr string, ClientPort string, nodeName string, entropyTimeout int, dsdv *additional.DSDVMap, rtimer int, pmsg *additional.PrivateMsgMap, files *additional.Files) {

	//setup the connection
	addr, err := net.ResolveUDPAddr("udp4", gossiperAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	connection, err := net.ListenUDP("udp4", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	//start listening for client messages
	go listenToClient(peers, ClientPort, nodeName, status, ipchan, msgs, connection, rtimer, pmsg, dsdv, files)
	//start anti entropy
	go antiEntropy(status, peers, connection, entropyTimeout)
	//strat listening for messages from peers
	listenToMessages(connection, peers, msgs, status, ipchan, dsdv, pmsg, nodeName, files)

}

//print status packet
func printStatus(status additional.StatusPacket, relay string, peers *additional.PeersMap) {
	thingToPrint := "STATUS from " + relay
	allstatus := status.Want
	length := len(allstatus)
	for counter := 0; counter < length; counter++ {
		ps := allstatus[counter]
		ID := fmt.Sprint(ps.NextID)
		thingToPrint = thingToPrint + " peer " + ps.Identifier + " nextID " + ID
	}
	fmt.Println(thingToPrint)
	fmt.Println("PEERS", peers.ToString())
}

//retruns a random peer
func getRandomPeer(allPeers map[string]string) string {
	if len(allPeers) == 0 {
		return "127.0.0.1:443"
	}
	seed := rand.NewSource(time.Now().UnixNano()) //seeding
	rng := rand.New(seed)
	randomNum := rng.Intn(len(allPeers))
	counter := 0
	for i := range allPeers {
		if counter == randomNum {
			return i

		}
		counter = counter + 1
	}
	return "ERROR NO PEER SELECTED THERE WAS A PROBLEM"
}

//sends the message to a random peer
func sendRandomPeer(peers *additional.PeersMap, conn *net.UDPConn, ipchan *additional.IPChanMap, packet additional.GossipPacket) string {
	allPeers := peers.LoadAll()
	if len(allPeers) > 0 {
		var chosen string
		seed := rand.NewSource(time.Now().UnixNano()) //seeding
		rng := rand.New(seed)
		randomNum := rng.Intn(len(allPeers))
		counter := 0
		for i := range allPeers {
			if counter == randomNum {
				chosen = i

			}
			counter = counter + 1
		}

		channel := ipchan.AddEntry(chosen)
		sendMsgToPeerExplicit(conn, chosen, packet)                                //send the message to the random peer
		go swMsgPeer(ipchan, channel, peers, conn, packet, chosen, waitingTimeout) //the timeout started
		fmt.Println("MONGERING with", chosen)
		return chosen
	}
	return ""
}

// we use this function to do parallel status update so that the main handler does not have to wait for the lock
func updateStatusP(status *additional.StatusMap, origin string, id uint32) {
	status.UpdateStatus(origin, id)
}

// inserts new origin with id 1
func insertNewOriginP(status *additional.StatusMap, origin string) {
	status.InsertNewOrigin(origin)
}

//function is used only when the simple flag is false; sends the client message to a random peer
func listenToClient(peers *additional.PeersMap, PORT string, nodeName string, status *additional.StatusMap, ipchan *additional.IPChanMap,
	msgs *additional.MsgMap, conn *net.UDPConn, rtimer int, pmsg *additional.PrivateMsgMap, dsdv *additional.DSDVMap, files *additional.Files) {
	//the ID of the messages that will be sent from the client
	id := additional.InitializeID()

	//start the contRouteRummor
	go contRouteRummor(peers, conn, rtimer, id, nodeName)

	//setup the connection
	addr, err := net.ResolveUDPAddr("udp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}

	connection, err := net.ListenUDP("udp4", addr)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer connection.Close() //close the connection in the end
	buffer := make([]byte, 1024)

	messageClient := &additional.Message{}

	for { //listens forever
		connection.ReadFromUDP(buffer)         //n, addr, err
		protobuf.Decode(buffer, messageClient) //decode the message

		//check the type of message from the client
		if len(*messageClient.Destination) > 0 { //it is a private message
			if len(*messageClient.File) > 0 { //requesting to download a file
				go files.RequestFile(*messageClient.Destination, *messageClient.File, *messageClient.Request, conn, dsdv, nodeName)

			} else { //private msg
				fmt.Println("CLIENT MESSAGE", messageClient.Text, "dest", *messageClient.Destination)
				//add client message to the private message map
				pmsg.AddMsg(nodeName, messageClient.Text)
				//send the private message to the next one
				ipToSend := dsdv.GetIP(*messageClient.Destination)
				privateMsg := &additional.PrivateMessage{Origin: nodeName, ID: 0, Text: messageClient.Text, Destination: *messageClient.Destination, HopLimit: 9} //consider hop as 10 since it is decremented here immediately
				gossipPack := additional.GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: privateMsg}
				sendMsgToPeerExplicit(conn, ipToSend, gossipPack)
			}

		} else {
			if len(*messageClient.File) > 0 { //sharing a file more or less
				go files.AddNewFile(*messageClient.File) //this will chop the file in to chunks and so on
			} else {
				//increment id
				msgID := id.IncID()

				//the unique name of the node nodeIP--nodeName
				uniqueNodeName := nodeName //gossiperAddr + "--" + nodeName it will crash in the automated test

				gossipPack := additional.GossipPacket{Simple: nil, Rumor: &additional.RumorMessage{ID: msgID, Origin: uniqueNodeName, Text: messageClient.Text}, Status: nil, Private: nil}

				msgs.AddMsg(uniqueNodeName, messageClient.Text, msgID)

				if msgID == 1 { //update our status for our messages
					status.InsertNewOrigin(uniqueNodeName)
				} else {
					status.UpdateStatus(uniqueNodeName, msgID)
				}

				fmt.Println("CLIENT MESSAGE", messageClient.Text) //Printing the client message

				//send the message to a random peer
				sendRandomPeer(peers, conn, ipchan, gossipPack)
			}
		}
	}
}

func main() {
	//Flag variables
	var UIPort int
	var gossipAddr string
	var nodeName string
	var peersIn string
	var simpleStat bool
	var antiEntropyTimeout int
	var rtimer int

	//Setting up the flags
	flag.IntVar(&UIPort, "UIPort", 6969, "User input port")
	flag.StringVar(&gossipAddr, "gossipAddr", "127.0.0.1:9696", "The gossipers own ip addr and port.")
	flag.StringVar(&nodeName, "name", "nodeA", "Name of the current gossiper node")
	flag.StringVar(&peersIn, "peers", "", "List of peers")
	flag.BoolVar(&simpleStat, "simple", false, "Simple flag makes the gossiper run in simple mode")
	flag.IntVar(&antiEntropyTimeout, "antiEntropy", 10, "anti entropy value")
	flag.IntVar(&rtimer, "rtimer", 0, "rtimer value")

	flag.Parse()

	//Initialization of the maps
	//Peers map
	peers := additional.NewPeersMap(peersIn)
	//Status map
	status := additional.NewStatusMap()
	//Ip channel map
	ipchan := additional.NewIPChanMap()
	//Messages map
	msgs := additional.NewMsgMap()
	//DSDV map
	dsdv := additional.NewDSDVMap()
	//private messages map
	pmsg := additional.NewPrivateMsgMap()
	//files struct
	files := additional.CreateFileHandler()

	PORT := ":" + strconv.Itoa(UIPort)

	if simpleStat { //go in broadcast mode
		//start the new thread for listening to the client
		go listenToClientSimple(peers, PORT, gossipAddr, nodeName)
		//listening for messages from peers
		listenToPeersSimple(peers, gossipAddr)
	} else {
		bootstrap(peers, status, msgs, ipchan, gossipAddr, PORT, nodeName, antiEntropyTimeout, dsdv, rtimer, pmsg, files)
	}

}
