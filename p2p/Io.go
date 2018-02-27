package p2p

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p-net"
)

var (
	errBlocked = errors.New("Blocked stream")
)

func newStreamMessenger(s net.Stream, maxSize int) streamMessenger {

	//default channel with initialized allowance
	channel := make(chan int, 1)
	channel <- 1

	return streamMessenger{s, make([]byte, binary.MaxVarintLen64),
		bufio.NewReader(s), nil, maxSize, channel}
}

type streamMessenger struct {
	Stream   net.Stream
	writeBuf []byte
	reader   *bufio.Reader
	readBuf  []byte
	maxSize  int
	blocked  chan int
}

func (sm *streamMessenger) lock(returnWhenBlocked bool) bool {
	//make sure we have the stream for us (as we need two writes, we need to make
	//sure noone else is writing)
	if returnWhenBlocked {
		select {
		case <-sm.blocked:
			//do nothing, as we are allowed to use the stream
		default:
			//someone elese uses the stream, return
			return false
		}
	} else {
		<-sm.blocked
	}
	return true
}

func (sm *streamMessenger) unlock() {
	sm.blocked <- 1
}

func (sm *streamMessenger) WriteMsg(msg Message, returnWhenBlocked bool) error {

	if !sm.lock(returnWhenBlocked) {
		return errBlocked
	}
	defer sm.unlock()

	return sm.writeMsg(msg)
}

func (sm *streamMessenger) writeMsg(msg Message) error {
	//send the message!
	//TODO: this is a allocation for each msg. Use buffer to make this more elegant
	data, err := Serialize(msg)
	if err != nil {
		return err
	}

	length := uint64(len(data))
	n := binary.PutUvarint(sm.writeBuf, length)
	_, err = sm.Stream.Write(sm.writeBuf[:n])
	if err != nil {
		return err
	}
	_, err = sm.Stream.Write(data)
	return err
}

func (sm *streamMessenger) ReadMsg(returnWhenBlocked bool) (Message, error) {

	if !sm.lock(returnWhenBlocked) {
		return nil, errBlocked
	}
	defer sm.unlock()

	return sm.readMsg()
}

func (sm *streamMessenger) readMsg() (Message, error) {

	length64, err := binary.ReadUvarint(sm.reader)
	if err != nil {
		return Error{err.Error()}, err
	}
	length := int(length64)
	if length < 0 || length > sm.maxSize {
		return Error{"Stream read failed: too short buffer"}, io.ErrShortBuffer
	}
	if len(sm.readBuf) < length {
		sm.readBuf = make([]byte, length)
	}
	buf := sm.readBuf[:length]
	_, err = io.ReadFull(sm.reader, buf)
	if err != nil {
		return Error{err.Error()}, err
	}
	return Deserialize(buf)
}

//sends a message and gets the imediate return
func (sm *streamMessenger) SendRequest(msg Message, returnWhenBlocked bool) (Message, error) {

	if !sm.lock(returnWhenBlocked) {
		return nil, errBlocked
	}
	defer sm.unlock()

	err := sm.writeMsg(msg)
	if err != nil {
		return nil, err
	}
	return sm.readMsg()
}

func (sm *streamMessenger) Close() error {
	sm.Stream.Close()
	close(sm.blocked)
	<-sm.blocked
	return nil
}

//Special messenger  for the stream participation logic.
//This messenger makes sure a connection always exist. Even if a stream is closed
//this messenger stays alive and creates a new stream with the next msg call.

type participationMessenger struct {
	host      *Host
	messenger streamMessenger
	role      string
	targetPid PeerID
	targetSid SwarmID
	valid     bool
	running   bool //default false is correct for defaultconstructed messengers
}

func newParticipationMessenger(host *Host, swarm SwarmID, target PeerID, role string) participationMessenger {

	return participationMessenger{host: host,
		role:      role,
		targetPid: target,
		targetSid: swarm,
		valid:     false,
		running:   true}
}

func (pm *participationMessenger) invalidate() {
	if pm.valid {
		pm.messenger.Close()
	}
	pm.valid = false
}

func (pm *participationMessenger) prepare() error {

	if !pm.running {
		return fmt.Errorf("Messenger already closed")
	}

	//make a new stream if needed
	if !pm.valid {
		stream, err := pm.host.host.NewStream(context.Background(), pm.targetPid.ID, swarmURI)
		if err != nil {
			return err
		}
		pm.messenger = newStreamMessenger(stream, 2<<(10*2)) //2mb per message max size
		pm.valid = true                                      //need to set valid to true here already, to ensure messenger is closed on further
		//invalid calls

		//communicate what we want
		msg := Participate{Swarm: pm.targetSid, Role: pm.role}
		err = pm.messenger.WriteMsg(msg, false)
		if err != nil {
			pm.invalidate()
			return err
		}

		//see if we are allowed to
		ret, err := pm.messenger.ReadMsg(false)
		if err != nil {
			pm.invalidate()
			return err
		}

		if ret.MessageType() != SUCCESS {
			pm.invalidate()
			return fmt.Errorf("Participation negotiation failed")
		}
	}
	return nil
}

func (pm *participationMessenger) WriteMsg(msg Message, returnWhenBlocked bool) error {

	err := pm.prepare()
	if err != nil {
		pm.invalidate()
		return err
	}

	err = pm.messenger.WriteMsg(msg, returnWhenBlocked)
	if err != nil && err != errBlocked {
		pm.invalidate()
	}
	return err
}

func (pm *participationMessenger) ReadMsg(returnWhenBlocked bool) (Message, error) {

	err := pm.prepare()
	if err != nil {
		pm.invalidate()
		return nil, err
	}

	msg, err := pm.messenger.ReadMsg(returnWhenBlocked)
	if err != nil {
		if err != errBlocked {
			pm.invalidate()
		}
		return nil, err
	}

	return msg, nil
}

func (pm *participationMessenger) SendRequest(msg Message, returnWhenBlocked bool) (Message, error) {

	err := pm.prepare()
	if err != nil {
		pm.invalidate()
		return nil, err
	}

	ret, err := pm.messenger.SendRequest(msg, returnWhenBlocked)
	if err != nil {
		if err != errBlocked {
			pm.invalidate()
		}
		return nil, err
	}

	return ret, nil
}

func (pm *participationMessenger) Close() {
	pm.invalidate()
	pm.running = false
}

//check if the
func (pm *participationMessenger) Connected() bool {
	return pm.running
}
