// Provides subscibing and publishng to events.
// Event do not have a clear peer as target, but propagate
// through the whole netork and are accepted from everyone who is subscribed
// to it. The propagation happen through the already existing connections.
package p2p

import (
	"context"
	"fmt"

	peer "github.com/libp2p/go-libp2p-peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

//small custom wrapper for message to expose custom Event type
//TODO: Expose User that created the event
type Event struct {
	Data   []byte
	Source PeerID
	Topic  string
}

//a small wrapper for subscription type
//supports custom message type
type Subscription struct {
	sub *pubsub.Subscription
}

//blocks till a event arrives, the context is canceled or the subscription itself
//is canceld
func (self Subscription) Next(ctx context.Context) (*Event, error) {
	msg, err := self.sub.Next(ctx)
	if err != nil {
		return nil, err
	}

	return &Event{msg.Data, PeerID(msg.GetFrom()), self.Topic()}, nil
}

//Cancels the subscription. Next will return with an error and no more events will
//be catched
func (self Subscription) Cancel() {
	self.sub.Cancel()
}

func (self Subscription) Topic() string {
	return self.sub.Topic()
}

func newHostEventService(host *Host) (*hostEventService, error) {

	ctx, cncl := context.WithCancel(context.Background())
	ps, err := pubsub.NewFloodSub(ctx, host.host)

	return &hostEventService{ps, cncl}, err
}

type hostEventService struct {
	service *pubsub.PubSub
	cancel  context.CancelFunc
}

func (self *hostEventService) Subscribe(topic string) (Subscription, error) {

	//TODO: add validator that checks user signature
	sub, err := self.service.Subscribe(topic)
	return Subscription{sub}, err
}

func (self *hostEventService) Publish(topic string, data []byte) error {

	//TODO: add signed user to the data
	return self.service.Publish(topic, data)
}

func (self *hostEventService) Stop() {
	self.cancel()
}

type swarmEventService struct {
	service *pubsub.PubSub
	swarm   *Swarm
}

func newSwarmEventService(swarm *Swarm) *swarmEventService {

	hostservice := swarm.host.Event
	return &swarmEventService{hostservice.service, swarm}
}

//Subscribe to a topic which requires a certain authorisation state
// - ReadOnly:  The topic is publishable by ReadOnly peers, hence everyone can publish on it
// - ReadWrite: The topic is only publishable by ReadWrite peers, hence publishing is only allowed by them
func (self *swarmEventService) Subscribe(topic string, required_auth AUTH_STATE) (Subscription, error) {

	if required_auth == AUTH_READONLY {
		topic = self.swarm.ID.Pretty() + `.` + topic

	} else if required_auth == AUTH_READWRITE {
		topic = self.swarm.ID.Pretty() + `.private.` + topic

	} else {
		return Subscription{}, fmt.Errorf("Unsupportet authorisation mode")
	}

	sub, err := self.service.Subscribe(topic)

	if required_auth == AUTH_READWRITE {
		swarm := self.swarm
		validator := func(ctx context.Context, id peer.ID, msg *pubsub.Message) bool {
			auth := swarm.PeerAuth(PeerID(id))
			return auth == AUTH_READWRITE
		}
		self.service.RegisterTopicValidator(topic, validator)
	}

	return Subscription{sub}, err
}

//Publish to a topic which requires a certain authorisation state. It must be the same state the listeners
//have subscribed with. If it is ReadWrite than they will only receive it if they have stored us with
//ReadWrite authorisation state.
func (self *swarmEventService) Publish(topic string, required_auth AUTH_STATE, data []byte) error {

	if required_auth == AUTH_READONLY {
		topic = self.swarm.ID.Pretty() + `.` + topic

	} else if required_auth == AUTH_READWRITE {
		topic = self.swarm.ID.Pretty() + `.private.` + topic

	} else {
		return fmt.Errorf("Unsupportet authorisation mode")
	}

	return self.service.Publish(topic, data)
}

func (self *swarmEventService) Stop() {}
