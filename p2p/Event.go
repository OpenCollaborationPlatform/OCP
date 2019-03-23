// Provides subscibing and publishng to events.
// Event do not have a clear peer as target, but propagate
// through the whole netork and are accepted from everyone who is subscribed
// to it. The propagation happen through the already existing connections.
package p2p

import (
	"context"

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
	sub           *pubsub.Subscription
	authorisation *authorizer
}

//blocks till a event arrives, the context is canceled or the subscription itself
//is canceld
func (self Subscription) Next(ctx context.Context) (*Event, error) {

	for {
		//get the message
		msg, err := self.sub.Next(ctx)
		if err != nil {
			return nil, err
		}

		//check if the authorisation of the caller checks out
		if self.authorisation == nil {
			// no authorisation required, return event!
			return self.eventFromMessage(msg), nil

		} else if self.authorisation.peerIsAuthorized(self.sub.Topic(), PeerID(msg.GetFrom())) {
			//the event publisher is allowed to post this event
			return self.eventFromMessage(msg), nil
		}
		//the posted message is not allowed to reach us. lets go on with waiting for a massage.
	}
}

func (self Subscription) eventFromMessage(msg *pubsub.Message) *Event {
	return &Event{msg.Data, PeerID(msg.GetFrom()), self.Topic()}
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
	ps, err := pubsub.NewFloodSub(ctx, host.host, pubsub.WithMessageSigning(true))

	return &hostEventService{ps, cncl}, err
}

type hostEventService struct {
	service *pubsub.PubSub
	cancel  context.CancelFunc
}

func (self *hostEventService) Subscribe(topic string) (Subscription, error) {

	//TODO: add validator that checks user signature
	sub, err := self.service.Subscribe(topic)
	return Subscription{sub, nil}, err
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

	topic = self.swarm.ID.Pretty() + `.` + topic
	sub, err := self.service.Subscribe(topic)

	//we have one authorizer per topic, as one can subscripe multiple times to a topic, and each time
	//theoretical with a different authorisation requriement
	auth := newAuthorizer()
	auth.addAuth(topic, required_auth, self.swarm)

	return Subscription{sub, auth}, err
}

//Publish to a topic which requires a certain authorisation state. It must be the same state the listeners
//have subscribed with. If it is ReadWrite than they will only receive it if they have stored us with
//ReadWrite authorisation state.
func (self *swarmEventService) Publish(topic string, data []byte) error {

	topic = self.swarm.ID.Pretty() + `.` + topic
	return self.service.Publish(topic, data)
}

func (self *swarmEventService) Stop() {}
