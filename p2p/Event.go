// Provides subscibing and publishng to events.
// Event do not have a clear peer as target, but propagate
// through the whole netork and are accepted from everyone who is subscribed
// to it. The propagation happen through the already existing connections.
package p2p

import (
	"context"
	"reflect"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/libp2p/go-libp2p-core/protocol"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/ugorji/go/codec"
)

const (
	eventProtocol = protocol.ID("/ocp/floodsub/1.0.0")
)

//Using msgpack for encoding to ensure, that all arguments that are handble by wamp are handled by the operation.
//This is poblematic with gob, as it needs to have many types registered, which is impossible to know for all
//the datatypes applications throw at us
var mph *codec.MsgpackHandle

func init() {
	mph = new(codec.MsgpackHandle)
	mph.WriteExt = true
	mph.MapType = reflect.TypeOf(map[string]interface{}(nil))
}

//small custom wrapper for message to expose custom Event type
type Event struct {
	Arguments []interface{}
	Source    PeerID
	Topic     string
}

//a small wrapper for subscription type
//supports custom message type
type Subscription struct {
	sub           *pubsub.Subscription
	authorisation *authorizer
	logger        hclog.Logger
}

//blocks till a event arrives, the context is canceled or the subscription itself
//is canceld
func (self Subscription) Next(ctx context.Context) (*Event, error) {

	for {
		//get the message
		msg, err := self.sub.Next(ctx)
		if err != nil {
			return nil, wrapInternalError(err, Error_Process)
		}

		//check if the authorisation of the caller checks out
		if self.authorisation == nil {
			// no authorisation required, return event!
			return self.eventFromMessage(msg)

		} else if self.authorisation.peerIsAuthorized(self.sub.Topic(), PeerID(msg.GetFrom())) {
			//the event publisher is allowed to post this event
			return self.eventFromMessage(msg)
		} else {
			self.logger.Debug("Received event from unauthorized peer", "topic", self.sub.Topic(), "peer", PeerID(msg.GetFrom()).Pretty())
		}
		//the posted message is not allowed to reach us. lets go on with waiting for a massage.
	}
}

func (self Subscription) eventFromMessage(msg *pubsub.Message) (*Event, error) {

	var arguments []interface{}
	err := codec.NewDecoderBytes(msg.Data, mph).Decode(&arguments)
	err = wrapInternalError(err, Error_Invalid_Data)

	return &Event{arguments, PeerID(msg.GetFrom()), self.Topic()}, err
}

//Cancels the subscription. Next will return with an error and no more events will
//be catched
func (self Subscription) Cancel() {
	self.sub.Cancel()
}

func (self Subscription) Topic() string {
	return self.sub.Topic()
}

//Filters subscriptions based on P2P authorisation
type SubscriptionFilter struct {
	authorisation *authorizer
	id            PeerID
}

// CanSubscribe returns true if the topic is of interest and we can subscribe to it
func (self SubscriptionFilter) CanSubscribe(topic string) bool {
	//we allow to subscribe to everything
	return true
}

// we filter every subscription that is not authorized
func (self SubscriptionFilter) FilterIncomingSubscriptions(sender peer.ID, subopts []*pb.RPC_SubOpts) ([]*pb.RPC_SubOpts, error) {

	peer := PeerID(sender)
	result := make([]*pb.RPC_SubOpts, 0)
	for _, subopt := range subopts {
		if self.authorisation.peerIsAuthorized(*subopt.Topicid, peer) {
			result = append(result, subopt)
		}
	}
	return result, nil
}

func newHostEventService(host *Host) (*hostEventService, error) {

	auth := newAuthorizer()
	//TODO: make filter work with swarm events
	//filter := SubscriptionFilter{auth, host.ID()}
	ctx, cncl := context.WithCancel(context.Background())
	ps, err := pubsub.NewGossipSub(ctx, host.host, pubsub.WithMessageSigning(true) /*, pubsub.WithSubscriptionFilter(filter)*/)
	if err != nil {
		err = wrapInternalError(err, Error_Setup)
	}

	return &hostEventService{ps, cncl, auth, host.logger.Named("Event")}, err
}

type hostEventService struct {
	service *pubsub.PubSub
	cancel  context.CancelFunc
	auth    *authorizer
	logger  hclog.Logger
}

func (self *hostEventService) Subscribe(topic string) (Subscription, error) {

	if !self.auth.isKnown(topic) {
		return Subscription{}, newInternalError(Error_Operation_Invalid, "Topic was not registered")
	}

	sub, err := self.service.Subscribe(topic)
	err = wrapConnectionError(err, Error_Process)

	return Subscription{sub, nil, self.logger}, err
}

func (self *hostEventService) Publish(topic string, args ...interface{}) error {

	if !self.auth.isKnown(topic) {
		return newInternalError(Error_Operation_Invalid, "Topic was not registered")
	}

	var data []byte
	err := codec.NewEncoderBytes(&data, mph).Encode(args)
	if err != nil {
		return wrapInternalError(err, Error_Invalid_Data)
	}

	err = self.service.Publish(topic, data)
	err = wrapConnectionError(err, Error_Process)

	return err
}

func (self *hostEventService) RegisterTopic(topic string) error {
	return utils.StackOnError(self.auth.addAuth(topic, AUTH_NONE, nil), "Unable  to add to authrisation handler")
}

func (self *hostEventService) Stop() {
	self.cancel()
}

type swarmEventService struct {
	service *pubsub.PubSub
	swarm   *Swarm
	auth    *authorizer
	logger  hclog.Logger
}

func newSwarmEventService(swarm *Swarm) *swarmEventService {

	hostservice := swarm.host.Event
	auth := hostservice.auth
	return &swarmEventService{hostservice.service, swarm, auth, swarm.logger.Named("Event")}
}

//Subscribe to a topic which requires a certain authorisation state
// - ReadOnly:  The topic is publishable by ReadOnly peers, hence everyone can publish on it
// - ReadWrite: The topic is only publishable by ReadWrite peers, hence publishing is only allowed by them
func (self *swarmEventService) Subscribe(topic string) (Subscription, error) {

	topic = self.swarm.ID.Pretty() + `.` + topic

	if !self.auth.isKnown(topic) {
		return Subscription{}, newInternalError(Error_Operation_Invalid, "Topic was not registered")
	}

	sub, err := self.service.Subscribe(topic)
	err = wrapConnectionError(err, Error_Process)

	return Subscription{sub, self.auth, self.logger}, err
}

//Publish to a topic which requires a certain authorisation state. It must be the same state the listeners
//have subscribed with. If it is ReadWrite than they will only receive it if they have stored us with
//ReadWrite authorisation state.
func (self *swarmEventService) Publish(topic string, args ...interface{}) error {

	topic = self.swarm.ID.Pretty() + `.` + topic

	if !self.auth.isKnown(topic) {
		return newInternalError(Error_Operation_Invalid, "Topic was not registered")
	}

	var data []byte
	err := codec.NewEncoderBytes(&data, mph).Encode(args)
	if err != nil {
		return wrapInternalError(err, Error_Invalid_Data)
	}

	err = self.service.Publish(topic, data)
	err = wrapConnectionError(err, Error_Process)

	return err
}

func (self *swarmEventService) RegisterTopic(topic string, required_auth AUTH_STATE) error {

	topic = self.swarm.ID.Pretty() + `.` + topic
	return utils.StackOnError(self.auth.addAuth(topic, required_auth, self.swarm), "Unable to add to authorizer")
}

func (self *swarmEventService) Stop() {
	//TODO: remove swarm from auth
}
