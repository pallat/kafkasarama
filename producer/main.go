package main

import (
	"log"
	"net/http"
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
	port                = os.Getenv("PORT")
)

func init() {
	if len(topic) == 0 {
		panic("no topic given to be consumed, please set the -topic flag")
	}
	if len(port) == 0 {
		port = "8080"
	}
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll

	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln(err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	go func() {
		http.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
			if len(client.Brokers()) > 0 {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("live"))
				return
			}

			w.WriteHeader(http.StatusServiceUnavailable)
		})
		http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
		})
		log.Println("Health check server listening on port", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Println("Health check server failed:", err)
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
