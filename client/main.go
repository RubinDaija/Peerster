package main

//Client
import (
	"Peerster/additional"
	"Peerster/protobuf"
	"encoding/hex"
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
	var dest string
	var file string
	var request string

	//Setting up the
	flag.IntVar(&UIPort, "UIPort", 6969, "Port of the gossiper")
	flag.StringVar(&msg, "msg", "Hello, World!", "Message to be sent.")
	flag.StringVar(&dest, "dest", "", "destination for the private message")
	flag.StringVar(&file, "file", "", "file to be index by the gossiper")
	flag.StringVar(&request, "request", "", "request a chunk or a metafile of this hash")

	flag.Parse()

	PORT := ":" + strconv.Itoa(UIPort)

	s, err := net.ResolveUDPAddr("udp4", PORT)
	c, err := net.DialUDP("udp4", nil, s)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer c.Close()

	hash, err := hex.DecodeString(request)
	if err != nil {
		fmt.Println(err)
		return
	}
	msgToSend := &additional.Message{Text: msg, Destination: &dest, File: &file, Request: &hash}

	//Using protobuf to encode the message
	packetBytes, err := protobuf.Encode(msgToSend)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = c.Write(packetBytes)
}
