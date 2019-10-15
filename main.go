package main

//TODO: rumormongering for client done , now only other peers left --dont forget to get now the peer relay address from the udp connection
//TODO: save all the messages sent since we need to send them to other peers as well -- easiest way is a map[strin <origin>][]string<array indexable by ID>
//      The only problem here is that the array needs to be flexible just like an array list in java
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
		gossipPack := &additional.GossipPacket{Simple: &additional.SimpleMessage{OriginalName: nodeName, RelayPeerAddr: gossiperAddr, Contents: message.Text}, Rumor: nil, Status: nil}
		gossipPack.Simple.RelayPeerAddr = gossiperAddr
		gossipPack.Simple.OriginalName = nodeName
		go sendMsgToAllPeers(peers, *gossipPack) //create a new thread so that this thread starts to listen again
	}
}

//This function sends the message to the peer given
func sendMessageToPeer(peer string, msg additional.GossipPacket) {
	addr, err := net.ResolveUDPAddr("udp4", peer)
	connection, err := net.DialUDP("udp4", nil, addr)
	packetBytes, err := protobuf.Encode(&msg)
	_, err = connection.Write(packetBytes)
	if err != nil {
		fmt.Println(err)
		return
	}
	connection.Close()
}

//This function sends the message to the peer given, using as the given port the one that we provide
func sendMsgToPeerExplicit(peer string, msg additional.GossipPacket, ourAddr string) {
	addr, err := net.ResolveUDPAddr("udp4", peer)
	addr2, err := net.ResolveUDPAddr("udp4", ourAddr)
	connection, err := net.DialUDP("udp4", addr2, addr)
	packetBytes, err := protobuf.Encode(&msg)
	_, err = connection.Write(packetBytes)
	if err != nil {
		fmt.Println(err)
		return
	}
	connection.Close()
}

//send and wait for message to peer - it sends the message (technically just waits) and then waits for the response from the peer
//recv is the channel that notifies this thread to die, whereas the del channel we send the ip so that
//the scheduling thread knows to delete that entry on the map and also to spawn a new instance of us
//random rumor mongering
func swMsgPeer(msg additional.SWResponse, recv *chan int, del *chan additional.SWResponse, timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	select {
	case <-ticker.C:
		close(*recv)
		*del <- msg
		return
	case <-*recv: //all recv channels are unique to one thread
		close(*recv)
		return
	}

}

//compares our status packet with their and if we have smth we send it otherwise it puts nil in the origin field of SRResponsePacket to signify that we should send a status packet
func compareStatus(ourStatus *additional.StatusMap, theirPack *additional.GossipPacket, response *chan additional.SRResponsePacket, addr string) {
	ourMap := ourStatus.GetStatusMap()
	theirMap := make(map[string]uint32)
	//convert their status packet to a map
	statusArr := theirPack.Status.Want
	for _, stat := range statusArr {
		theirMap[stat.Identifier] = stat.NextID
	}

	//comparison time
	if len(theirMap) > len(ourMap) { //they have more elems than us so we just send a status packet to get smth; this is more efficient than to do the comparison and do this check
		*response <- additional.SRResponsePacket{IP: addr, Origin: "SsendstatuspacketS", MessageID: 1} //notify the controller to send a status packet
		return
	}

	for key, value := range ourMap {
		_, ok := theirMap[key]
		if !ok { //they do not have our message
			*response <- additional.SRResponsePacket{IP: addr, Origin: key, MessageID: 1}
			return
		} else if value > theirMap[key] {
			*response <- additional.SRResponsePacket{IP: addr, Origin: key, MessageID: theirMap[key]}
			return
		} else if value < theirMap[key] {
			*response <- additional.SRResponsePacket{IP: addr, Origin: "SsendstatuspacketS", MessageID: 1} //notify the controller to send a status packet
			return
		}
	}
	*response <- additional.SRResponsePacket{IP: addr, Origin: "AOKAOKAOK", MessageID: 1} //if we are equal
}

//the antiEntropy function will be run immediately on start up. TODO check if time should be seconds or milliseconds
func antiEntropy(status *additional.StatusMap, peers *additional.PeersMap, sendRes *chan string, timeout int) {
	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	select {
	case <-ticker.C:
		peerMp := peers.LoadAll()
		if len(peerMp) > 0 {
			//choose random peer
			randomNum := rand.Intn(len(peerMp))
			counter := 0
			choosenPeer := ""
			for i := range peerMp {
				if counter == randomNum {
					choosenPeer = i
					break
				}
				counter = counter + 1
			}
			//send the peer choosen
			*sendRes <- choosenPeer
		}

	}
}

//this function listen to messages from peers and also manages the messages
func listenToPeers(peers *additional.PeersMap, status *additional.StatusMap, gossiperAddr string, ClientPort string, nodeName string, entropyTimeout int) {
	timeout := 10
	allMsg := make(map[string][]string) //origin, array of messages based on id, do ID - 1 when indexing as ids start from 1
	ipToChan := make(map[string]chan int)
	delChan := make(chan additional.SWResponse)
	responseStatus := make(chan additional.SRResponsePacket)
	antiEntropyChan := make(chan string)
	messagesClient := make(chan additional.SWResponse, 10)

	go listenToClient(peers, ClientPort, gossiperAddr, nodeName, &messagesClient, status) //start listening for client messages

	go antiEntropy(status, peers, &antiEntropyChan, entropyTimeout) //start anti entropy

	var latestRummor additional.GossipPacket //latest rummor received
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

	buffer := make([]byte, 1024)

	gossipPack := &additional.GossipPacket{}

	for { //listens forever
		_, recvFrom, _ := connection.ReadFromUDP(buffer) //n, addr, err
		protobuf.Decode(buffer, gossipPack)              //decode the message
		connection.Close()                               //close the connection when we about to send a message, cause we need the port to send messages

		currStatusPacket := status.CreateStatusPacket() //current status packet
		currPeers := peers.LoadAll()                    //current peers map[string]string
		peersString := peers.ToString()

		//0. check if we know the ip port combo or we should add it
		_, ok := currPeers[recvFrom.String()]
		if !ok {
			peers.StoreNotExists(recvFrom.String())
		}

		//1. Check the type of the message - status or message
		if gossipPack.Rumor != nil {
			//send a status packet
			sendMsgToPeerExplicit(recvFrom.String(), additional.GossipPacket{Simple: nil, Rumor: nil, Status: currStatusPacket}, gossiperAddr)

			//print the rummor message and peers
			fmt.Println("RUMOR origin", gossipPack.Rumor.Origin, "from", recvFrom.String(), "ID", gossipPack.Rumor.ID, "contents", gossipPack.Rumor.Text)
			fmt.Println("PEERS", peersString)

			//2. Check if it is a new message or a completely new peer
			value, existance := allMsg[gossipPack.Rumor.Origin]
			if existance { //it is a peer that we already know
				if uint32(len(value)) == gossipPack.Rumor.ID { //it is a new message and it is in order
					latestRummor = *gossipPack

					allMsg[gossipPack.Rumor.Origin] = append(value, gossipPack.Rumor.Text)                                     //add message to the map of messages
					go updateStatusP(status, gossipPack.Rumor.Origin, gossipPack.Rumor.ID)                                     //update the status in a parallel manner
					channelToUse := make(chan int, 2)                                                                          // I am not sure if 1 will block :P
					randomPeer := getRandomPeer(currPeers)                                                                     //getting a random peer to start rummor mongering with
					ipToChan[randomPeer] = channelToUse                                                                        //set the channel in the map
					sendMsgToPeerExplicit(randomPeer, *gossipPack, gossiperAddr)                                               //send the message to the random peer
					go swMsgPeer(additional.SWResponse{IP: randomPeer, Packet: *gossipPack}, &channelToUse, &delChan, timeout) //the timeout started
					fmt.Println("MONGERING with", randomPeer)
				}

			} else { //it is a new completely new peer
				if gossipPack.Rumor.ID == 1 { //we are getting the first message
					latestRummor = *gossipPack

					var messages []string
					allMsg[gossipPack.Rumor.Origin] = append(messages, gossipPack.Rumor.Text) //adding it to the messages that we know
					go insertNewOriginP(status, gossipPack.Rumor.Origin)                      //updated the status
					//start random rummor mongering
					channelToUse := make(chan int, 2)                                                                          // I am not sure if 1 will block :P
					randomPeer := getRandomPeer(currPeers)                                                                     //getting a random peer to start rummor mongering with
					ipToChan[randomPeer] = channelToUse                                                                        //set the channel in the map
					sendMsgToPeerExplicit(randomPeer, *gossipPack, gossiperAddr)                                               //send the message to the random peer
					go swMsgPeer(additional.SWResponse{IP: randomPeer, Packet: *gossipPack}, &channelToUse, &delChan, timeout) //the timeout started
					fmt.Println("MONGERING with", randomPeer)
				}
			}

		} else { //it is a status packet -- simply compare the packets then we check later the result through the channel
			go printStatus(*gossipPack.Status, recvFrom.String(), peers) //print status
			compareStatus(status, gossipPack, &responseStatus, recvFrom.String())
			//we also check if it is a response to one of our messages or just anti-entropy or the message has timeouted
			value, existance := ipToChan[recvFrom.String()]
			if existance { //it exists
				value <- 1                          //notify the node that it does exist
				delete(ipToChan, recvFrom.String()) //delete the entry
			}

		}

		entropyRes := chanString(antiEntropyChan)
		deleteRes := chanSWResponse(delChan)
		responseRes := chanSRResponse(responseStatus)
		clientRes := chanSWResponse(messagesClient)

		currStatusPacket = status.CreateStatusPacket()
		if len(entropyRes) > 0 { //send the message to the peer set by anti entropy
			sendMsgToPeerExplicit(entropyRes, additional.GossipPacket{Simple: nil, Rumor: nil, Status: currStatusPacket}, gossiperAddr)
		}
		for len(deleteRes.IP) > 0 { //delete the ones that have timeouted and send those messages again
			delete(ipToChan, deleteRes.IP) //deleting the entry
			//start random rummor mongering
			channelToUse := make(chan int, 2)
			randomPeer := getRandomPeer(currPeers)
			ipToChan[randomPeer] = channelToUse
			sendMsgToPeerExplicit(randomPeer, deleteRes.Packet, gossiperAddr)
			go swMsgPeer(additional.SWResponse{IP: randomPeer, Packet: deleteRes.Packet}, &channelToUse, &delChan, timeout)
			fmt.Println("MONGERING with", randomPeer)

			deleteRes = chanSWResponse(delChan) //check if there are more

		}
		for responseRes.MessageID > 0 {
			if responseRes.Origin == "SsendstatuspacketS" { //we send a status packet back
				sendMsgToPeerExplicit(responseRes.IP, additional.GossipPacket{Simple: nil, Rumor: nil, Status: currStatusPacket}, gossiperAddr)
			} else if responseRes.Origin == "AOKAOKAOK" { //we are equal
				fmt.Println("IN SYNC WITH", responseRes.IP)
				coin := rand.Int() % 2
				if coin == 0 { //head we rummor monger
					//start random rummor mongering
					channelToUse := make(chan int, 2)
					randomPeer := getRandomPeer(currPeers)
					ipToChan[randomPeer] = channelToUse
					sendMsgToPeerExplicit(randomPeer, latestRummor, gossiperAddr) //we use the latest rummor cause sending another packet requires exhaustive handling of shit
					go swMsgPeer(additional.SWResponse{IP: randomPeer, Packet: latestRummor}, &channelToUse, &delChan, timeout)
					fmt.Println("FLIPPED COIN sending rumor to", randomPeer)
					fmt.Println("MONGERING with", randomPeer)
				} //tails we dont do anything
			} else { // send a message from us
				message := (allMsg[responseRes.Origin])[(responseRes.MessageID - 1)]
				packet := additional.GossipPacket{Simple: nil, Rumor: &additional.RumorMessage{Origin: responseRes.Origin, ID: responseRes.MessageID, Text: message}, Status: nil}
				sendMsgToPeerExplicit(responseRes.IP, packet, gossiperAddr)
				channelToUse := make(chan int, 2)
				ipToChan[responseRes.IP] = channelToUse
				go swMsgPeer(additional.SWResponse{IP: responseRes.IP, Packet: packet}, &channelToUse, &delChan, timeout)
			}
			responseRes = chanSRResponse(responseStatus) //check for more
		}
		for len(clientRes.IP) > 0 { //check if we received from client
			sendMsgToPeerExplicit(clientRes.IP, clientRes.Packet, gossiperAddr)
			channelToUse := make(chan int, 2)
			ipToChan[clientRes.IP] = channelToUse
			if clientRes.Packet.Rumor.ID == 1 {
				var messages []string
				allMsg[clientRes.Packet.Rumor.Origin] = append(messages, clientRes.Packet.Rumor.Text)
			} else {
				ourMessages := allMsg[clientRes.Packet.Rumor.Origin]
				allMsg[clientRes.Packet.Rumor.Origin] = append(ourMessages, clientRes.Packet.Rumor.Text)
			}

			go swMsgPeer(additional.SWResponse{IP: clientRes.IP, Packet: clientRes.Packet}, &channelToUse, &delChan, timeout)

			clientRes = chanSWResponse(messagesClient)
		}
		//TODO we need also a channel for the server but I am not bitching now

		connection, err = net.ListenUDP("udp4", addr) //reconnect to the port to listen to msg
	}

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
	randomNum := rand.Intn(len(allPeers))
	counter := 0
	for i := range allPeers {
		if counter == randomNum {
			return i

		}
		counter = counter + 1
	}
	return "ERROR NO PEER SELECTED THERE WAS A PROBLEM"
}

//read channels in a nonblocking manner
func chanSWResponse(channel chan additional.SWResponse) additional.SWResponse {
	select {
	case res := <-channel:
		return res
	default:
		return additional.SWResponse{IP: "", Packet: additional.GossipPacket{Simple: nil, Rumor: nil, Status: nil}} //check if IP is empty to know that we did not receive anything
	}
}

func chanSRResponse(channel chan additional.SRResponsePacket) additional.SRResponsePacket {
	select {
	case res := <-channel:
		return res
	default:
		return additional.SRResponsePacket{IP: "", Origin: "", MessageID: 0} //check message ID to check if we read something
	}
}

func chanInt(channel chan int) int {
	select {
	case res := <-channel:
		return res
	default:
		return 0 //if we read 0 we know we did not receive anything
	}
}

func chanString(channel chan string) string {
	select {
	case res := <-channel:
		return res
	default:
		return "" //if we read "" we know we did not receive anything
	}
}

func chanGossipPacket(channel chan additional.GossipPacket) additional.GossipPacket {
	select {
	case res := <-channel:
		return res
	default:
		return additional.GossipPacket{Simple: nil, Rumor: nil, Status: nil}
	}
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
func listenToClient(peers *additional.PeersMap, PORT string, gossiperAddr string, nodeName string, toSend *chan additional.SWResponse, status *additional.StatusMap) {
	//the ID of the messages that will be sent from the client
	var msgID uint32 = 1 //first message id is 1

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

		//get the random peer to send the message to
		choosenPeer := ""
		allPeers := peers.LoadAll()
		if len(allPeers) == 0 {
			choosenPeer = "127.0.0.1:443" //temporary
		} else {
			randomNum := rand.Intn(len(allPeers))
			counter := 0
			for i := range allPeers {
				if counter == randomNum {
					choosenPeer = i
					break
				}
				counter = counter + 1
			}
		}

		//the unique name of the node nodeIP--nodeName
		uniqueNodeName := gossiperAddr + "--" + nodeName

		gossipPack := additional.GossipPacket{Simple: nil, Rumor: &additional.RumorMessage{ID: msgID, Origin: uniqueNodeName, Text: messageClient.Text}, Status: nil}
		msgID = msgID + 1

		if msgID == 1 { //update our status for our messages
			status.InsertNewOrigin(uniqueNodeName)
		} else {
			status.UpdateStatus(uniqueNodeName, msgID)
		}

		fmt.Println("CLIENT MESSAGE", messageClient.Text) //Printing the client message
		*toSend <- additional.SWResponse{IP: choosenPeer, Packet: gossipPack}

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

	//Setting up the flags
	flag.IntVar(&UIPort, "UIPort", 6969, "User input port")
	flag.StringVar(&gossipAddr, "gossipAddr", "127.0.0.1:9696", "The gossipers own ip addr and port.")
	flag.StringVar(&nodeName, "name", "nodeA", "Name of the current gossiper node")
	flag.StringVar(&peersIn, "peers", "", "List of peers")
	flag.BoolVar(&simpleStat, "simple", false, "Simple flag makes the gossiper run in simple mode")
	flag.IntVar(&antiEntropyTimeout, "antiEntropy", 10, "anti entropy value")

	flag.Parse()

	//Pre-preperations
	peers := additional.NewPeersMap(peersIn) // value, exists := peers[value]
	status := additional.NewStatusMap()      //create a new status map

	PORT := ":" + strconv.Itoa(UIPort)

	if simpleStat { //go in broadcast mode
		go listenToClientSimple(peers, PORT, gossipAddr, nodeName) //start the new thread for listening to the client
		listenToPeersSimple(peers, gossipAddr)                     //listening for messages from peers
	} else {
		listenToPeers(peers, status, gossipAddr, PORT, nodeName, antiEntropyTimeout)
	}

	// for { //if the main thread stops then the whole code stops and the other threads get terminated

	// }

}
