package register

import (
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type Consumer struct {
	ready chan struct{}
}

func NewConsumer() *Consumer {
	return &Consumer{ready: make(chan struct{})}
}

func (consumer *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}
func (*Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (*Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
consume:
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				slog.Info("message channel was closed")
				break consume
			}

			if err := Handle(msg.Value); err != nil {

			}

			fmt.Printf("Message topic:%q partition:%d offset:%d message:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)

			sess.MarkMessage(msg, "")
			// sess.Commit()
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-sess.Context().Done():
			break consume
		}
	}
	return sess.Context().Err()
}

func (c *Consumer) NewReady() {
	c.ready = make(chan struct{})
}

func (c *Consumer) Ready() <-chan struct{} {
	return c.ready
}

func Handle(b []byte) error {
	return nil
}
