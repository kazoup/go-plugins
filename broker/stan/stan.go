package stan

import (
	"strings"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/broker/codec/json"
	"github.com/micro/go-micro/cmd"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats"
)

type nbroker struct {
	clusterID string
	clientID  string
	addrs     []string
	conn      stan.Conn
	opts      broker.Options
	stanOpts  stan.Options
}

type subscriber struct {
	topic    string
	sub      stan.Subscription
	opts     broker.SubscribeOptions
	stanOpts stan.SubscriptionOptions
}

type publication struct {
	topic   string
	msg     *broker.Message
	stanMsg *stan.Msg
}

func init() {
	cmd.DefaultBrokers["stan"] = NewBroker
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.Options{
		Codec: json.NewCodec(),
	}

	for _, o := range opts {
		o(&options)
	}

	stanOptions := stan.DefaultOptions

	nb := &nbroker{
		clusterID: "test-cluster",
		clientID:  "client",
		addrs:     setAddrs(options.Addrs),
		opts:      options,
		stanOpts:  stanOptions,
	}

	nb.setExtraOptions()

	return nb
}

// micro/go-micro/broker.Publiction interface implementation
func (p *publication) Topic() string { return p.topic }

func (p *publication) Message() *broker.Message { return p.msg }

// TODO check if this is correct
func (p *publication) Ack() error { return p.stanMsg.Ack() }

// micro/go-micro/broker.Subscriber interface implementation
func (s *subscriber) Options() broker.SubscribeOptions { return s.opts }

func (s *subscriber) Topic() string { return s.topic }

func (s *subscriber) Unsubscribe() error { return s.sub.Unsubscribe() }

// micro/go-micro/broker.Broker interface implementation
// TODO implement
func (n *nbroker) Options() broker.Options { return n.opts }

// TODO implement
func (n *nbroker) Connect() error {
	return nil
}

// TODO implement
func (n *nbroker) Disconnect() error { return nil }

// TODO implement
func (n *nbroker) Address() string { return "" }

// TODO implement
func (n *nbroker) Init(...broker.Option) error { return nil }

// TODO implement
func (n *nbroker) Publish(string, *broker.Message, ...broker.PublishOption) error { return nil }

// TODO implement
func (n *nbroker) Subscribe(string,
	broker.Handler,
	...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

// TODO implement
func (n *nbroker) String() string { return "" }

// helper functions

// TODO refactor this to return []stan.Option
func (n *nbroker) setExtraOptions() {
	ctx := n.opts.Context

	clusterID := ctx.Value("clusterID")
	if clusterID, ok := clusterID.(string); ok && clusterID != "" {
		n.clusterID = clusterID
	}

	clientID := ctx.Value("clientID")
	if clientID, ok := clientID.(string); ok && clientID != "" {
		n.clientID = clientID
	}

	natsURL := ctx.Value("natsURL")
	if natsURL, ok := natsURL.(string); ok && natsURL != "" {
		n.stanOpts.NatsURL = natsURL
	}

	connectTimeout := ctx.Value("connectTimeout")
	if connectTimeout, ok := connectTimeout.(time.Duration); ok && connectTimeout != time.Duration(0) {
		n.stanOpts.ConnectTimeout = connectTimeout
	}

	ackTimeoutout := ctx.Value("ackTimeoutout")
	if ackTimeoutout, ok := ackTimeoutout.(time.Duration); ok && ackTimeoutout != time.Duration(0) {
		n.stanOpts.AckTimeout = ackTimeoutout
	}

	discoverPrefix := ctx.Value("discoverPrefix")
	if discoverPrefix, ok := discoverPrefix.(string); ok && discoverPrefix != "" {
		n.stanOpts.DiscoverPrefix = discoverPrefix
	}

	maxPubAcksInflight := ctx.Value("maxPubAcksInflight")
	if maxPubAcksInflight, ok := maxPubAcksInflight.(int); ok && maxPubAcksInflight != 0 {
		n.stanOpts.MaxPubAcksInflight = maxPubAcksInflight
	}
}

func setAddrs(addrs []string) []string {
	var cAddrs []string
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{nats.DefaultURL}
	}
	return cAddrs
}
