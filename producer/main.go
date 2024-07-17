package main

import (
	"log"
	"os"

	_ "net/http/pprof"

	"github.com/rcrowley/go-metrics"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	broker    = os.Getenv("BROKER")
	version   = sarama.DefaultVersion.String()
	topic     = os.Getenv("TOPIC")
	producers = 1
	verbose   = false

	recordsNumber int64 = 1
	recordsRate         = metrics.GetOrRegisterMeter("records.rate", nil)
)

func init() {
	if len(topic) == 0 {
		panic("no topic given to be consumed, please set the -topic flag")
	}
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092", "localhost:9093", "localhost:9094"}, config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	for range 10 {
		msg := &sarama.ProducerMessage{Topic: "my-topic", Value: sarama.StringEncoder("testing 123")}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("FAILED to send message: %s\n", err)
		} else {
			log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
		}
	}
}
