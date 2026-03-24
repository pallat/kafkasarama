package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/pallat/kafkasarama/consumergroup/register"
)

// Sarama configuration options
var (
	brokers = os.Getenv("BROKERS")
	version = sarama.DefaultVersion.String()
	group   = os.Getenv("GROUP")
	topics  = os.Getenv("TOPICS")
	verbose = false
	oldest  = false
	port    = os.Getenv("PORT")
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
	if len(port) == 0 {
		port = "8080"
	}
}

func main() {
	consumer := register.NewConsumer()

	config := sarama.NewConfig()
	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	client, err := sarama.NewClient(strings.Split(brokers, ","), config)
	if err != nil {
		log.Panicf("new client: %v", err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			log.Panicf("closing client: %v", err)
		}
	}()

	groupClient, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		log.Panicf("new consumer group: %v", err)
	}
	defer func() {
		if err = groupClient.Close(); err != nil {
			log.Panicf("closing consumer group: %v", err)
		}
	}()

	go func() {
		http.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
			// Now we can use the client to check partitions
			partitions, err := client.Partitions(strings.Split(topics, ",")[0])
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("can not connect"))
				return
			}
			if len(partitions) > 0 {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("live"))
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not live"))
		})

		http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-consumer.Ready():
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ready"))
			default:
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("not ready"))
			}
		})

		// For backward compatibility
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})

		slog.Info("Health check server listening on port " + port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			slog.Error("Health check server failed: " + err.Error())
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	ctx, gracefully := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		for {
			if err := groupClient.Consume(ctx, strings.Split(topics, ","), consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("consume: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if err := ctx.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Info("the consumer context has cancelled for gracefully shutting down")
					return
				}
				slog.Error(ctx.Err().Error())
				return
			}
			slog.Info("rebalancing...")
			consumer.NewReady()
		}
	}()

	<-consumer.Ready()

	slog.Info("consumer up and running...")
	sigCtx, unregistered := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer unregistered()
keepRunning:
	for {
		select {
		case <-ctx.Done():
			slog.Info("terminating: consumer context cancel")
			break keepRunning
		case <-sigCtx.Done():
			slog.Info("terminating: via signal")
			unregistered()
			break keepRunning
		}
	}
	gracefully()
	wg.Wait() // waiting for gracefully consumer stopping
}
