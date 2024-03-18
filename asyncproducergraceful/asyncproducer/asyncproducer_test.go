package asyncproducer

import (
	"errors"
	"os"
	"os/signal"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func TestProcess(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	asyncProducer := mocks.NewAsyncProducer(t, config)

	err := errors.New("wowww")
	a := asyncProducer.ExpectInputAndFail(err)

	p := New("topicTest1", a)

	chMsg := make(chan Message)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go p.Process(chMsg, signals)

	chMsg <- Message{
		Key:   nil,
		Value: []byte("message1"),
	}

	<-asyncProducer.Errors()
	signals <- os.Interrupt
}
