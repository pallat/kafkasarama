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
	producer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("testing 123")}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}
