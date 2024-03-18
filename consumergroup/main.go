package main

// SIGUSR1 toggle the pause/resume consumption
import (
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

	group, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := group.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
}
