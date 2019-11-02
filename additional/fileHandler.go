package additional

import (
	"Peerster/protobuf"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

var hopLim uint32 = 9
var fileReqTimeout = 5

func check(err error) {
	if err != nil {
		panic(err)
	}
}

//uncompleteFile is an internal struct to keep track of uncompleted files
//the stings are the chuncks hash value
type uncompleteFile struct {
	filesToRec    []string //files yet to receive
	filesRecieved []string //files received
	waitingMeta   bool     // true if we are waiting for the metahash, false otherwise
	fromWhom      string   //from whom are we asking
	filename      string   //the file name
	metahash      []byte   // this files metahash
}

//uncompleteFiles is a struct full of uncompleted files
type uncompleteFiles struct {
	files map[string]uncompleteFile //string refers to hash we are waiting for in this case
}

//File has the general file info
type File struct {
	filename string
	fileSize int64
	metahash []byte
}

//Files is the greneral data structure with all the information and visible everywhere
type Files struct {
	sync.RWMutex
	allChunks map[string]string //all the chuncks that we have present hash and filename
	allFiles  map[string]File
	uFiles    uncompleteFiles
	hashChan  map[string]chan int //hash of the chunck and the corresponding channel waiting for them
}

//CreateFileHandler creates the files struct to manage all the files
func CreateFileHandler() *Files {
	uncompFiles := make(map[string]uncompleteFile)
	uncompFileStruct := uncompleteFiles{files: uncompFiles}
	allchunks := make(map[string]string)
	allfiles := make(map[string]File)
	hashchan := make(map[string]chan int)
	return &Files{allChunks: allchunks, allFiles: allfiles, uFiles: uncompFileStruct, hashChan: hashchan}

}

//AddNewFile adds the new file to the appropriate data structures and cuts it into chunks
func (f *Files) AddNewFile(fileName string) {
	//the slice that will hold all the sha256 calcs of chuncks
	var metafile []byte

	f.Lock()

	//open the file and get the size
	file, err := os.Open("_SharedFiles/" + fileName)
	defer file.Close()
	check(err)
	fileInfo, err := file.Stat()
	check(err)
	fileSize := fileInfo.Size()
	fileSizeLeft := fileSize

	//start parsing the file into chunks
	for fileSizeLeft > 0 {
		//read chunk
		chunk := make([]byte, 8192)
		noRead, err := file.Read(chunk)
		check(err)
		//compute hash
		hash := sha256.Sum256(chunk[:noRead])
		hashSlice := hash[:noRead]
		//put hash together for metafile
		metafile = append(metafile, hashSlice...)
		//save the chunck in the cache
		chunkFile, err := os.Create("cache/" + hex.EncodeToString(hashSlice))
		check(err)
		_, err = chunkFile.Write(chunk)
		check(err)
		chunkFile.Close()

		fileSizeLeft = fileSizeLeft - 8192

		//add chunk to our datastructure Files
		f.allChunks[hex.EncodeToString(hashSlice)] = fileName
	}
	//save calculate the resulting metahash and save its file in the cache
	metahash := sha256.Sum256(metafile)
	metahashSlice := metahash[:]
	metaHashFile, err := os.Create("cache/" + hex.EncodeToString(metahashSlice))
	check(err)
	_, err = metaHashFile.Write(metafile)
	check(err)
	metaHashFile.Close()

	//add metahash to our datastructure Files also add general file info
	f.allChunks[hex.EncodeToString(metahashSlice)] = fileName
	currFile := File{filename: fileName, fileSize: fileSize, metahash: metahashSlice}
	f.allFiles[fileName] = currFile

	f.Unlock()

}

//SendNWait this function sends our gossip datarequest and waits for the reply
func SendNWait(conn *net.UDPConn, pck GossipPacket, timeout int, recvdChann *chan int, dsdv *DSDVMap) {
	peer := dsdv.GetIP(pck.DataRequest.Destination)
	addr, err := net.ResolveUDPAddr("udp4", peer)
	if err != nil {
		fmt.Println(err)
		return
	}
	packetBytes, err := protobuf.Encode(&pck)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = conn.WriteToUDP(packetBytes, addr)
	if err != nil {
		fmt.Println(err)
		return
	}
	ticker := time.NewTicker(time.Duration(timeout) * time.Second)
	for { //sends messages till we receive a reply
		peer = dsdv.GetIP(pck.DataRequest.Destination)
		select {
		case <-ticker.C:
			_, err = conn.WriteToUDP(packetBytes, addr)
			if err != nil {
				fmt.Println(err)
				return
			}
		case <-*recvdChann: //all recv channels are unique to one thread
			close(*recvdChann)
			return
		}
	}
}

//CheckNReturnChunk checks if we have a particular chunk and then we return it, if we don't have it then the result will be empty
func (f *Files) CheckNReturnChunk(dreq DataRequest, dsdv *DSDVMap, conn *net.UDPConn) {
	var result []byte
	hash := hex.EncodeToString(dreq.HashValue)
	f.RLock()
	_, existence := f.allChunks[hash]
	if existence {
		fileChunk := make([]byte, 8192)
		file, err := os.Open("cache/" + hash)
		check(err)
		noRead, err := file.Read(fileChunk)
		check(err)
		result = fileChunk[:noRead]
		file.Close()
	}
	f.RUnlock()
	dataRep := DataReply{Origin: dreq.Origin, Destination: dreq.Destination, HopLimit: hopLim, HashValue: dreq.HashValue, Data: result}
	ip2send := dsdv.GetIP(dreq.Origin)
	pck := GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: nil, DataReply: &dataRep}
	addr, err := net.ResolveUDPAddr("udp4", ip2send)
	check(err)
	packetBytes, err := protobuf.Encode(&pck)
	check(err)
	_, err = conn.WriteToUDP(packetBytes, addr)
	check(err)

}

//StoreNewChunk checks the sha256 of the chunk and then if we need it stores it
//also it sends a request if we need more, hence when we get a message we check the destination and the hop amount
// if it is all okay and the mssage is for us then we simply pass it to this fcn
func (f *Files) StoreNewChunk(dataRpl DataReply, conn *net.UDPConn, dsdv *DSDVMap) {
	f.Lock()
	defer f.Unlock()

	hash := dataRpl.HashValue
	chunk := dataRpl.Data
	hashStr := hex.EncodeToString(hash)
	uncompFile, existance := f.uFiles.files[hashStr]
	//check if it is a hash we are looking for
	if existance {
		//notify and delete the entry for this chunk for sendnwait
		value, _ := f.hashChan[hashStr]
		value <- 1
		delete(f.hashChan, hashStr)

		//new channel to be used by sendnwait provided that we need to send more messages
		newChan := make(chan int, 2)

		//Check if the hash is correct
		sha := sha256.Sum256(chunk)
		shaStr := hex.EncodeToString(sha[:])
		//hashes don't match, data is corrupt
		if hashStr != shaStr {
			dr := DataRequest{Origin: dataRpl.Origin, Destination: uncompFile.fromWhom, HopLimit: hopLim}
			pck := GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: &dr, DataReply: nil}
			f.hashChan[hashStr] = newChan
			go SendNWait(conn, pck, fileReqTimeout, &newChan, dsdv)
			return

		}
		//add this hash to the all hashes present map
		f.allChunks[hashStr] = uncompFile.filename

		//remove the existing hash ucompfile from the map, we still do have the ucompfile though
		delete(f.uFiles.files, hashStr)

		//check if it is the metahash
		if uncompFile.waitingMeta {
			fmt.Println("DOWNLOADING metafile of", uncompFile.filename, "from", uncompFile.fromWhom)
			uncompFile.waitingMeta = false
			//save the metahash file in the cache
			metaFile, err := os.Create("cache/" + hashStr)
			check(err)
			_, err = metaFile.Write(dataRpl.Data)
			check(err)
			metaFile.Close()

			//take all the hashes and store them in a slice
			var allHash []string
			for i := 0; i < len(dataRpl.Data); i = i + 32 {
				tmpSha := dataRpl.Data[i : i+32]
				tmpStr := hex.EncodeToString(tmpSha)
				allHash = append(allHash, tmpStr)
			}
			uncompFile.filesToRec = allHash[1:] //saving the rest of the hashes but the first one in the slice that indicates the hashes to be received

			//create a entry for the current hash required to save the uncompleted file
			f.uFiles.files[allHash[0]] = uncompFile

			//send the new request
			dr := DataRequest{Origin: dataRpl.Origin, Destination: uncompFile.fromWhom, HopLimit: hopLim}
			pck := GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: &dr, DataReply: nil}
			f.hashChan[allHash[0]] = newChan
			go SendNWait(conn, pck, fileReqTimeout, &newChan, dsdv)
		} else {
			//Save the file
			chunk, err := os.Create("cache/" + hashStr)
			check(err)
			_, err = chunk.Write(dataRpl.Data)
			check(err)
			chunk.Close()

			//save the hash in the array for processing when we are done and update the ones to be received (remove the one we received)
			uncompFile.filesRecieved = append(uncompFile.filesRecieved, hashStr)
			uncompFile.filesToRec = remove(uncompFile.filesToRec, 0)
			fmt.Println("DOWNLOADING", uncompFile.filename, "chunk", len(uncompFile.filesRecieved), "from", uncompFile.fromWhom)

			//check if there are hashes left to get and get the next hash to request
			if len(uncompFile.filesToRec) > 0 {
				//get the next hash and send it to be requested
				nextHash := uncompFile.filesToRec[0]
				//create a entry for the current hash required to save the uncompleted file
				f.uFiles.files[nextHash] = uncompFile

				//send the new request
				dr := DataRequest{Origin: dataRpl.Origin, Destination: uncompFile.fromWhom, HopLimit: hopLim}
				pck := GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: &dr, DataReply: nil}
				f.hashChan[nextHash] = newChan
				go SendNWait(conn, pck, fileReqTimeout, &newChan, dsdv)
			} else { //file it tutocompleto
				fmt.Println("RECONSTRUCTED file", uncompFile.filename)
				fileChunk := make([]byte, 8192)
				//create the new file
				completeFile, err := os.Create("Downloads/" + hashStr)
				check(err)

				//Create the file completely
				for _, fhash := range uncompFile.filesRecieved {
					//read the file chunk to write
					tmpfile, err := os.Open("cache/" + fhash)
					check(err)
					noRead, err := tmpfile.Read(fileChunk)
					check(err)
					result := fileChunk[:noRead]
					//write this chunk to the file
					_, err = completeFile.Write(result)
					check(err)
					tmpfile.Close()

				}
				completeFileInfo, err := completeFile.Stat()
				check(err)
				//add the completed file to a file struct
				fileStruct := File{filename: uncompFile.filename, fileSize: completeFileInfo.Size(), metahash: uncompFile.metahash}
				f.allFiles[uncompFile.filename] = fileStruct
				completeFile.Close()
			}
		}
	}
}

//SendRequestedChunk sends the requested file chunk to the peer; supposes hop limit is already decremented and also that we are the destination
//We are also supposed to pass the peer to whom the reply should be sent to. //TODO: i dont know if we need this
func (f *Files) SendRequestedChunk(dr DataRequest, conn *net.UDPConn, peer string) {
	f.RLock()
	hashStr := hex.EncodeToString(dr.HashValue)
	var resultChunk []byte
	//check if we have the chunk
	_, existance := f.allChunks[hashStr]
	if existance {
		fileChunk := make([]byte, 8192)
		file, err := os.Open("cache/" + hashStr)
		check(err)
		noRead, err := file.Read(fileChunk)
		check(err)
		resultChunk = fileChunk[:noRead]
		file.Close()
	}
	dreply := DataReply{Origin: dr.Origin, Destination: dr.Destination, HopLimit: hopLim, HashValue: dr.HashValue, Data: resultChunk}
	packet := GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: nil, DataReply: &dreply}

	addr, err := net.ResolveUDPAddr("udp4", peer)
	check(err)
	packetBytes, err := protobuf.Encode(&packet)
	check(err)
	_, err = conn.WriteToUDP(packetBytes, addr)
	check(err)
	f.RUnlock()

}

//RequestFile is called at the very beginnign when the file is requested
//It creates the uncompFile struct provides it the file name, towhom, metahash and then also sends the request
func (f *Files) RequestFile(dest string, fileName string, hash []byte, conn *net.UDPConn, dsdv *DSDVMap, ourName string) {
	f.Lock()
	defer f.Unlock()
	//Create the uncomplete file struct and add it to the datastruct
	filestorec := make([]string, 1)
	filesreced := make([]string, 1)
	uFile := uncompleteFile{filesToRec: filestorec, filesRecieved: filesreced, waitingMeta: true, fromWhom: dest, filename: fileName, metahash: hash}
	hashStr := hex.EncodeToString(hash)
	f.uFiles.files[hashStr] = uFile

	//send the request
	//new channel to be used by sendnwait provided that we need to send more messages
	newChan := make(chan int, 2)
	dr := DataRequest{Origin: ourName, Destination: dest, HopLimit: hopLim}
	pck := GossipPacket{Simple: nil, Rumor: nil, Status: nil, Private: nil, DataRequest: &dr, DataReply: nil}
	f.hashChan[hashStr] = newChan
	go SendNWait(conn, pck, fileReqTimeout, &newChan, dsdv)
}

//taken from https://stackoverflow.com/questions/37334119/how-to-delete-an-element-from-a-slice-in-golang
func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}
