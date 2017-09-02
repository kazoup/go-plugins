package stan

import (
	"strings"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/broker/codec/json"
	"github.com/micro/go-micro/cmd"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats"
)

type nbroker struct {
	clusterID string
	clientID  string
	addrs     []string
	conn      stan.Conn
	opts      broker.Options
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

	nb := &nbroker{
		clusterID: "test-cluster",
		clientID:  "",
		addrs:     setAddrs(options.Addrs),
		opts:      options,
	}

	nb.setClientAndClusterID()

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
func (n *nbroker) Options() broker.Options { return n.opts }

func (n *nbroker) Connect() error {
	if n.conn != nil {
		return nil
	}

	// TODO figure out how to set the underlying nats connection options
	// suspect before connecting NatsConn() will be nil, and I don't know
	// if I can set these values after calling stan.Connect
	//n.conn.NatsConn().Opts.Servers = n.addrs
	//n.conn.NatsConn().Opts.Secure = n.opts.Secure
	//n.conn.NatsConn().Opts.TLSConfig = n.opts.TLSConfig

	//if n.conn.NatsConn().Opts.TLSConfig != nil {
	//	n.conn.NatsConn().Opts.Secure = true
	//}

	sc, err := stan.Connect(n.clusterID, n.clientID, n.extraOptions()...)
	if err != nil {
		return err
	}

	n.conn = sc
	return nil
}

func (n *nbroker) Disconnect() error { return n.conn.Close() }

func (n *nbroker) Address() string {
	if n.conn != nil && n.conn.NatsConn() != nil && n.conn.NatsConn().IsConnected() {
		return n.conn.NatsConn().ConnectedUrl()
	}
	if len(n.addrs) > 0 {
		return n.addrs[0]
	}

	return ""
}

func (n *nbroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&n.opts)
	}

	n.setClientAndClusterID()

	n.addrs = setAddrs(n.opts.Addrs)
	return nil
}

func (n *nbroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	b, err := n.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	return n.conn.Publish(topic, b)
}

// TODO implement
func (n *nbroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	s := &subscriber{topic: topic}

	for _, o := range opts {
		o(&s.opts)
	}

	fn := func(msg *stan.Msg) {
		var m broker.Message
		if err := n.opts.Codec.Unmarshal(msg.Data, &m); err != nil {
			return
		}

		if err := handler(&publication{msg: &m, topic: msg.Subject, stanMsg: msg}); err == nil {
			// if the manual ack mode is not set, Ack() will just return error
			msg.Ack()
		}
	}

	var err error
	if len(s.opts.Queue) > 0 {
		s.sub, err = n.conn.QueueSubscribe(topic, s.opts.Queue, fn, s.extraOptions()...)
	} else {
		s.sub, err = n.conn.Subscribe(topic, fn, s.extraOptions()...)
	}
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (n *nbroker) String() string { return "stan" }

func (n *nbroker) extraOptions() []stan.Option {
	ctx := n.opts.Context
	opts := make([]stan.Option, 0)

	natsURL := ctx.Value("natsURL")
	if natsURL, ok := natsURL.(string); ok && natsURL != "" {
		opts = append(opts, stan.NatsURL(natsURL))
	}

	connectTimeout := ctx.Value("connectTimeout")
	if connectTimeout, ok := connectTimeout.(time.Duration); ok && connectTimeout != time.Duration(0) {
		opts = append(opts, stan.ConnectWait(connectTimeout))
	}

	ackTimeout := ctx.Value("ackTimeout")
	if ackTimeout, ok := ackTimeout.(time.Duration); ok && ackTimeout != time.Duration(0) {
		opts = append(opts, stan.PubAckWait(ackTimeout))
	}

	maxPubAcksInflight := ctx.Value("maxPubAcksInflight")
	if maxPubAcksInflight, ok := maxPubAcksInflight.(int); ok && maxPubAcksInflight != 0 {
		opts = append(opts, stan.MaxPubAcksInflight(maxPubAcksInflight))
	}

	return opts
}

func (s *subscriber) extraOptions() []stan.SubscriptionOption {
	ctx := s.opts.Context
	opts := make([]stan.SubscriptionOption, 0)

	maxInFlight := ctx.Value("maxInFlight")
	if maxInFlight, ok := maxInFlight.(int); ok {
		opts = append(opts, stan.MaxInflight(maxInFlight))
	}

	ackWait := ctx.Value("ackWait")
	if ackWait, ok := ackWait.(time.Duration); ok {
		opts = append(opts, stan.AckWait(ackWait))
	}

	startAt := ctx.Value("startAt")
	if startAt, ok := startAt.(pb.StartPosition); ok {
		opts = append(opts, stan.StartAt(startAt))
	}

	startAtSequence := ctx.Value("startAtSequence")
	if startAtSequence, ok := startAtSequence.(uint64); ok {
		opts = append(opts, stan.StartAtSequence(startAtSequence))
	}

	startTime := ctx.Value("startTime")
	if startTime, ok := startTime.(time.Time); ok {
		opts = append(opts, stan.StartAtTime(startTime))
	}

	startAtTimeDelta := ctx.Value("startAtTimeDelta")
	if startAtTimeDelta, ok := startAtTimeDelta.(time.Duration); ok {
		opts = append(opts, stan.StartAtTimeDelta(startAtTimeDelta))
	}

	startWithLastReceived := ctx.Value("startWithLastReceived")
	if startWithLastReceived, ok := startWithLastReceived.(bool); ok && startWithLastReceived {
		opts = append(opts, stan.StartWithLastReceived())
	}

	deliverAllAvailable := ctx.Value("deliverAllAvailable")
	if deliverAllAvailable, ok := deliverAllAvailable.(bool); ok && deliverAllAvailable {
		opts = append(opts, stan.DeliverAllAvailable())
	}

	setManualAckMode := ctx.Value("setManualAckMode")
	if setManualAckMode, ok := setManualAckMode.(bool); ok && setManualAckMode || s.Options().AutoAck {
		opts = append(opts, stan.SetManualAckMode())
	}

	durableName := ctx.Value("durableName")
	if durableName, ok := durableName.(string); ok && durableName != "" {
		opts = append(opts, stan.DurableName(durableName))
	}

	return opts
}

func (n *nbroker) setClientAndClusterID() {
	ctx := n.opts.Context

	// some defaults
	n.clusterID = "test-cluster"
	n.clientID = ""

	clusterID := ctx.Value("clusterID")
	if clusterID, ok := clusterID.(string); ok && clusterID != "" {
		n.clusterID = clusterID
	}

	clientID := ctx.Value("clientID")
	if clientID, ok := clientID.(string); ok && clientID != "" {
		n.clientID = clientID
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
