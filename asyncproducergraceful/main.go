package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	_ "net/http/pprof"

	metrics "github.com/rcrowley/go-metrics"

	"github.com/IBM/sarama"

	"kafka/cluster/asyncproducertesting/asyncproducer"
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

	MessageStart = 1
	MessageEnd   = 5
)

func init() {
	if len(topic) == 0 {
		panic("no topic given to be consumed, please set the -topic flag")
	}
}

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("close: %s\n", err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	processor := asyncproducer.New(topic, producer)

	chMessage := make(chan asyncproducer.Message)

	var (
		wg                        sync.WaitGroup
		successes, producerErrors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
			fmt.Printf("success %d\n", successes)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			producerErrors++
			log.Printf("error %d %s\n", producerErrors, err)
		}
	}()

	go processor.Process(chMessage, signals)

	for i := MessageStart; i <= MessageEnd; i++ {
		chMessage <- asyncproducer.Message{
			Key:   nil,
			Value: []byte(fmt.Sprintf("testing %d", i)),
		}
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, producerErrors)
}
