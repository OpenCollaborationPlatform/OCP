//This file provides serializable messages. It is modelled after (well, copied from)
//wamp messages
package p2p

type MessageType int

// Message is a generic container for a p2p message.
type Message interface {
	MessageType() MessageType
}

// Dict is a dictionary that maps keys to objects in a p2p message.
type Dict map[string]interface{}

// List represents a list of items in a p2p message.
type List []interface{}

// Message Codes and Direction
const (
	ERROR       MessageType = 1
	PARTICIPATE MessageType = 2
	SUCCESS     MessageType = 3
	EVENT       MessageType = 4
)

var mtStrings = map[MessageType]string{
	ERROR:       "ERROR",
	PARTICIPATE: "PARTICIPATE",
	SUCCESS:     "SUCCESS",
	EVENT:       "EVENT",
}

// String returns the message type string.
func (mt MessageType) String() string { return mtStrings[mt] }

// NewMessage returns an empty message of the type specified.
func NewMessage(t MessageType) Message {
	switch t {
	case ERROR:
		return &Error{}
	case PARTICIPATE:
		return &Participate{}
	case SUCCESS:
		return &Success{}
	case EVENT:
		return &Event{}
	}
	return nil
}

// Sent if any operation went wrong
type Error struct {
	Reason string
}

func (msg Error) MessageType() MessageType { return ERROR }

// Sent by a Peer to say for which swarm the given stream is intended
type Participate struct {
	Swarm SwarmID
	Role  string
}

func (msg Participate) MessageType() MessageType { return PARTICIPATE }

// Simple success indicator for any type of calls
type Success struct{}

func (msg Success) MessageType() MessageType { return SUCCESS }

// Event msg which transports events
type Event struct {
	Uri    string
	KwArgs Dict
	Args   List
}

func (msg Event) MessageType() MessageType { return EVENT }
