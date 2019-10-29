package additional

import (
	"sync"
)

//This file is used only to handle private messages and their appropriate functions

//PrivateMsgMap - has all the messages with all origins
type PrivateMsgMap struct {
	sync.RWMutex
	messages map[string][]string
}

//NewPrivateMsgMap - initializing the msg map
func NewPrivateMsgMap() *PrivateMsgMap {
	variable := make(map[string][]string)
	return &PrivateMsgMap{messages: variable}
}

//AddMsg - add a message to the message map
func (pmm *PrivateMsgMap) AddMsg(origin string, msg string) {
	pmm.Lock()
	value, _ := pmm.messages[origin]
	pmm.messages[origin] = append(value, msg)
	pmm.Unlock()
}

//GetMsg - returns the message from the given origin and index
func (pmm *PrivateMsgMap) GetMsg(origin string, index uint32) string {
	var toReturn string
	pmm.RLock()
	value, _ := pmm.messages[origin]
	toReturn = value[index]
	pmm.RUnlock()
	return toReturn
}
