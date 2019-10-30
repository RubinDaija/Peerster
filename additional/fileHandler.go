package additional

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

//uncompleteFile is an internal struct to keep track of uncompleted files
//the stings are the chuncks hash value
type uncompleteFile struct {
	filesToRec    map[string]string //files yet to receive
	filesRecieved []string          //files received
}

//uncompleteFiles is a struct full of uncompleted files
type uncompleteFiles struct {
	files map[string]uncompleteFile //string refers to file name in this case
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
}

//AddNewFile adds the new file to the appropriate data structures and cuts it into chunks
func (f *Files) AddNewFile(fileName string) {
	//the slice that will hold all the sha256 calcs of chuncks
	var metafile []byte

	f.Lock()

	//open the file and get the size
	file, err := os.Open("_SharedFiles" + fileName)
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
		chunkFile, err := os.Create("cache" + hex.EncodeToString(hashSlice))
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
	metaHashFile, err := os.Create("cache" + hex.EncodeToString(metahashSlice))
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

//CheckNReturnChunk checks if we have a particular chunk and then we return it
func (f *Files) CheckNReturnChunk(hash string) []byte {
	var result []byte
	f.RLock()
	_, existence := f.allChunks[hash]
	if existence {
		fileChunk := make([]byte, 8192)
		file, err := os.Open("cache" + hash)
		check(err)
		noRead, err := file.Read(fileChunk)
		check(err)
		result = fileChunk[:noRead]
		file.Close()
	}
	f.RUnlock()
	return result
}

//StoreNewChunk checks the sha256 of the chunk and then if we need it stores it
func (f *Files) StoreNewChunk(chunk []byte, hash []byte) {

}
