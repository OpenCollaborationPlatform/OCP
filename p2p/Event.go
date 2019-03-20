// Swarm: event functions
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
	sub *pubsub.Subscription
}

//blocks till a event arrives, the context is canceled or the subscription itself
//is canceld
func (self Subscription) Next(ctx context.Context) (*Event, error) {
	msg, err := self.sub.Next(ctx)
	if err != nil {
		return nil, err
	}

	return &Event{msg.Data, PeerID{msg.GetFrom()}, self.Topic()}, nil
}

//Cancels the subscription. Next will return with an error and no more events will
//be catched
func (self Subscription) Cancel() {
	self.sub.Cancel()
}

func (self Subscription) Topic() string {
	return self.sub.Topic()
}

//EventService must be an interface to allow specialized swarm implementation
type EventService interface {
	Publish(topic string, data []byte) error
	Suscribe(topic string) (Subscription, error)
	Stop()
}

func NewEventService(host *Host) (EventService, error) {

	ctx, cncl := context.WithCancel(context.Background())
	ps, err := pubsub.NewGossipSub(ctx, host.host)

	return &hostEventService{ps, cncl}, err
}

type hostEventService struct {
	service *pubsub.PubSub
	cancel  context.CancelFunc
}

func (self *hostEventService) Suscribe(topic string) (Subscription, error) {

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
	service *hostEventService
	id      SwarmID
}

func (self *swarmEventService) Suscribe(topic string) (Subscription, error) {

	topic = self.id.Pretty() + `/` + topic

	//TODO: add validator that checks user signature and swarm rights
	return self.service.Suscribe(topic)
}

func (self *swarmEventService) Publish(topic string, data []byte) error {

	topic = self.id.Pretty() + `/` + topic

	//TODO: add signed user to the data
	return self.service.Publish(topic, data)
}
