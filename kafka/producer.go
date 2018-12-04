package kafka

import (
	"github.com/Shopify/sarama"
	"time"
)

type Producer struct {
	producer sarama.SyncProducer
	Address []string
}

func (this *Producer) Connect() error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	producer, err := sarama.NewSyncProducer(this.Address, config)
	if err == nil {
		this.producer = producer
	}
	return err
}

func (this *Producer) DisConnection()  {
	if this.producer != nil {
		this.producer.Close()
		this.producer = nil
	}
}

func (this *Producer) SendMessage(topic string, message string) (partition int32, offset int64, err error)  {
	return this.producer.SendMessage(&sarama.ProducerMessage{
		Topic:topic,
		Value:sarama.ByteEncoder(message),
	})
}
