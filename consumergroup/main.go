package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	brokers  = os.Getenv("BROKERS")
	version  = sarama.DefaultVersion.String()
	group    = os.Getenv("GROUP")
	topics   = os.Getenv("TOPICS")
	assignor = os.Getenv("ASSIGNOR")
	verbose  = false
	oldest   = true
	groupID  = "group3"
)

func init() {
	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topics) == 0 {
		panic("no topics given to be consumed, please set the -topics flag")
	}

	if len(group) == 0 {
		panic("no Kafka consumer group defined, please set the -group flag")
	}
}

func main() {
	config := sarama.NewConfig()

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	group, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := group.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	ctx := context.Background()
	for {
		topics := strings.Split(topics, ",")
		handler := exampleConsumerGroupHandler{}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
consume:
	for {
		select {
		case msg := <-claim.Messages():
			fmt.Printf("Message topic:%q partition:%d offset:%d message:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
			sess.MarkMessage(msg, "")
		case <-sess.Context().Done():
			fmt.Println("rebalance")
			break consume
		}
	}
	return sess.Context().Err()
}
