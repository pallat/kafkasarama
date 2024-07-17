package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"errors"
	"log"
	"log/slog"
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
	consumer := register.NewConsumer()

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("new client: %v", err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			log.Panicf("closing client: %v", err)
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	ctx, gracefully := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, strings.Split(topics, ","), consumer); err != nil {
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
	if err = client.Close(); err != nil {
		log.Panicf("closing client: %v", err)
	}
}
