package stan

import (
	"strings"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/broker/codec/json"
	"github.com/micro/go-micro/cmd"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats"
)

type nbroker struct {
	addrs    []string
	conn     stan.Conn
	opts     broker.Options
	stanOpts stan.Options
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

	// TODO parse the options context field and replace the defaults
	stanOptions := stan.DefaultOptions

	return &nbroker{
		addrs:    setAddrs(options.Addrs),
		opts:     options,
		stanOpts: stanOptions,
	}
}

// micro/go-micro/broker.Publiction interface implementation
// TODO implement
func (p *publication) Topic() string { return "" }

// TODO implement
func (p *publication) Message() *broker.Message { return &broker.Message{} }

// TODO implement
func (p *publication) Ack() error { return nil }

// micro/go-micro/broker.Subscriber interface implementation
// TODO implement
func (s *subscriber) Options() broker.SubscribeOptions { return broker.SubscribeOptions{} }

// TODO implement
func (s *subscriber) Topic() string { return "" }

// TODO implement
func (s *subscriber) Unsubscribe() error { return nil }

// micro/go-micro/broker.Broker interface implementation
// TODO implement
func (n *nbroker) Options() broker.Options { return broker.Options{} }

// TODO implement
func (n *nbroker) Connect() error { return nil }

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
