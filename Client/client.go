package main

//Client
import (
	"Peerster/additional"
	"Peerster/protobuf"
	"flag"
	"fmt"
	"net"
	"strconv"
)

func createSimpleMessage(msg string) *additional.SimpleMessage {
	return nil
}

func main() {
	//Flag variables
	var UIPort int
	var msg string

	//Setting up the
	flag.IntVar(&UIPort, "UIPort", 6969, "Port of the gossiper")
	flag.StringVar(&msg, "msg", "Hello, World!", "Message to be sent.")

	flag.Parse()

	PORT := ":" + strconv.Itoa(UIPort)

	s, err := net.ResolveUDPAddr("udp4", PORT)
	c, err := net.DialUDP("udp4", nil, s)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer c.Close()

	//Using protobuf to encode the message
	msgToSend := &additional.Message{Text: msg}
	packetBytes, err := protobuf.Encode(msgToSend)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = c.Write(packetBytes)
}
