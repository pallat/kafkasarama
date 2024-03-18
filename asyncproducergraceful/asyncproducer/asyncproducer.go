package asyncproducer

import (
	"os"

	"github.com/IBM/sarama"
)

type Producer struct {
	topic         string
	asyncProducer sarama.AsyncProducer
}

func New(topic string, asyncProducer sarama.AsyncProducer) Producer {
	return Producer{topic: topic, asyncProducer: asyncProducer}
}

type Message struct {
	Key   []byte
	Value []byte
}

func (p Producer) Process(messageChan chan Message, interruptSig chan os.Signal) {
	for {
		select {
		case message := <-messageChan:
			p.asyncProducer.Input() <- &sarama.ProducerMessage{Topic: p.topic, Key: sarama.ByteEncoder(message.Key), Value: sarama.ByteEncoder(message.Value)}
		case <-interruptSig:
			p.asyncProducer.AsyncClose()
			return
		}
	}
}
