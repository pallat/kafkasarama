package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"log"
	"os"
	"strings"

	_ "net/http/pprof"

	metrics "github.com/rcrowley/go-metrics"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	brokers   = os.Getenv("BROKERS")
	version   = sarama.DefaultVersion.String()
	topic     = os.Getenv("TOPIC")
	producers = 1
	verbose   = false

	recordsNumber int64 = 1
	recordsRate         = metrics.GetOrRegisterMeter("records.rate", nil)

	MessageStart = 301
	MessageEnd   = 311
)

func init() {
	if len(topic) == 0 {
		panic("no topic given to be consumed, please set the -topic flag")
	}
}

func main() {
	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	var enqueued, producerErrors int
	for range 5 {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder("testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}
