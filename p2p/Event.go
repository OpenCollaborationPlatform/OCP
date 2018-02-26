// Swarm: event functions
package p2p

import (
	"fmt"
	"log"
)

func (s *Swarm) PostEvent(uri string, kwargs Dict) {

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	//lets call all our peers! Note: Could happen that we don't have a event messenger
	//if the other peer allows us as read only!
	for key, data := range s.peers {
		if data.Event.Connected() {
			fmt.Printf("Send event to %s", key.Pretty())
			go func() {
				err := data.Event.WriteMsg(Event{uri, kwargs, List{}}, false)
				if err != nil {
					fmt.Printf("Error writing Event: %s", err)
				}
			}()
		}
	}
}

func (s *Swarm) RegisterEventChannel(uri string, channel chan Dict) {

	s.eventLock.Lock()
	defer s.eventLock.Unlock()

	slice, ok := s.eventChannels[uri]
	if !ok {
		s.eventChannels[uri] = make([]chan Dict, 0)
		slice, _ = s.eventChannels[uri]
	}

	s.eventChannels[uri] = append(slice, channel)
}

func (s *Swarm) RegisterEventCallback(uri string, function func(Dict)) {

	s.eventLock.Lock()
	defer s.eventLock.Unlock()

	slice, ok := s.eventCallbacks[uri]
	if !ok {
		s.eventCallbacks[uri] = make([]func(Dict), 0)
		slice, _ = s.eventCallbacks[uri]
	}

	s.eventCallbacks[uri] = append(slice, function)
}

//this function handles events. Note that this stream not only has Event messages,
//but is also used for some other internal messages, e.g. for data distribution
func (s *Swarm) handleEventStream(pid PeerID, messenger streamMessenger) {

	go func() {
		msg, err := messenger.ReadMsg(false)

		if err != nil {
			log.Printf("Error reading message: %s", err.Error())
			return
		}

		//verify the sender of the event and see if he is allowed to send events
		//TODO: check signature of message and see if the sender is allowed to send it

		switch msg.MessageType() {

		case EVENT:
			event := msg.(*Event)
			fmt.Printf("Event received: %s", event.Uri)
			s.eventLock.RLock()
			callbacks, ok := s.eventCallbacks[event.Uri]
			if ok {
				for _, call := range callbacks {
					go call(event.KwArgs)
				}
			}
			channels, ok := s.eventChannels[event.Uri]
			if ok {
				for _, channel := range channels {
					go func() { channel <- event.KwArgs }()
				}
			}
			s.eventLock.RUnlock()
		}
	}()
}
