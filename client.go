package kafkago

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Client interface {
	Disconnect()
	Consume(map[string]chan []byte)
	Produce(topic string, message []byte)
}

type KafkaConfig struct {
	producer         bool
	consumer         bool
	server           string
	groupId          string
	clientId         string
	autoOffsetReset  string
	securityProtocol string
	saslMechanism    string
	saslUser         string
	saslPass         string
	serviceDLQ       string
}

type KafkaClient struct {
	Consumer         *kafka.Consumer
	Producer         *kafka.Producer
	dlqProducer      *kafka.Producer
	LogFunction      func(string)
	ErrorLogFunction func(string)
}

func NewClient(c *KafkaConfig, logFunction func(string), errorFunction func(string)) (*KafkaClient, error) {
	client := &KafkaClient{LogFunction: logFunction, ErrorLogFunction: errorFunction}
	if c.consumer {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": c.server,
			"group.id":          c.groupId,
			"client.id":         c.clientId,
			"auto.offset.reset": c.autoOffsetReset,
			"security.protocol": c.securityProtocol,
			"sasl.mechanism":    c.saslMechanism,
			"sasl.username":     c.saslUser,
			"sasl.password":     c.saslPass,
		})

		if err != nil {
			return nil, err
		}
		client.Consumer = consumer
	}

	if c.producer {
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": c.server,
			"client.id":         c.clientId,
			"security.protocol": c.securityProtocol,
			"sasl.mechanism":    c.saslMechanism,
			"sasl.username":     c.saslUser,
			"sasl.password":     c.saslPass,
		})

		if err != nil {
			return nil, err
		}
		client.Producer = producer
		go func() {
			for e := range client.Producer.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					// The message delivery report, indicating success or
					// permanent failure after retries have been exhausted.
					// Application level retries won't help since the client
					// is already configured to do that.
					m := ev
					if m.TopicPartition.Error != nil {
						client.ErrorLogFunction(fmt.Sprintf("KAFKA ERROR: DELIVERY FAILED %v\n", m.TopicPartition.Error))
						client.sendToDLQ(m)
					}
				}
			}
		}()
	}
	return client, nil
}

func (k *KafkaClient) sendToDLQ(m *kafka.Message) {

}

func (k *KafkaClient) Produce(topic string, message []byte) {
	kMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}
	err := k.Producer.Produce(kMessage, nil)
	if err != nil {
		k.ErrorLogFunction("KAFKA ERROR: " + err.Error())
		k.sendToDLQ(kMessage)
	}
	k.Producer.Flush(15 * 1000)
}

func (k *KafkaClient) Consume(channels map[string]chan []byte) {
	topics := []string{}
	for t, _ := range channels {
		topics = append(topics, t)
	}
	err := k.Consumer.SubscribeTopics(topics, nil)
	if err != nil {
		panic(err)
	}
	for {
		msg, err := k.Consumer.ReadMessage(-1)
		k.LogFunction(fmt.Sprintf("%v %v %v %v", "KAFKA -> OFFSET LATEST:", msg.TopicPartition.Offset, "PARTITION:", msg.TopicPartition.Partition))
		if err == nil {
			topic := *msg.TopicPartition.Topic
			channels[topic] <- msg.Value
		} else {
			k.ErrorLogFunction(fmt.Sprintf("KAFKA ERROR: CONSUME %v (%v)\n", err, msg))
		}
	}
}

func (k *KafkaClient) Disconnect() {
	if k.Consumer != nil {
		err := k.Consumer.Close()
		if err != nil {
			k.ErrorLogFunction(err.Error())
		}
	}
	if k.Producer != nil {
		k.Producer.Close()
	}
}
