package kafkago

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type DLQMessage struct {
	Topic string `json:"topic"`
	Body  string `json:"body"`
}

type Client interface {
	Disconnect()
	Consume(map[string]chan []byte)
	Produce(topic string, message []byte)
}

type KafkaConfig struct {
	Producer         bool
	Consumer         bool
	Server           string
	GroupId          string
	ClientId         string
	AutoOffsetReset  string
	SecurityProtocol string
	SaslMechanism    string
	SaslUser         string
	SaslPass         string
	ServiceDLQ       string
}

type KafkaClient struct {
	Consumer         *kafka.Consumer
	Producer         *kafka.Producer
	dlqProducer      *kafka.Producer
	TopicDLQ         string
	LogFunction      func(string)
	ErrorLogFunction func(string)
}

func NewClient(c *KafkaConfig, logFunction func(string), errorFunction func(string)) (*KafkaClient, error) {
	client := &KafkaClient{
		TopicDLQ:         c.ServiceDLQ,
		LogFunction:      logFunction,
		ErrorLogFunction: errorFunction,
	}
	if c.Consumer {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": c.Server,
			"group.id":          c.GroupId,
			"client.id":         c.ClientId,
			"auto.offset.reset": c.AutoOffsetReset,
			"security.protocol": c.SecurityProtocol,
			"sasl.mechanism":    c.SaslMechanism,
			"sasl.username":     c.SaslUser,
			"sasl.password":     c.SaslPass,
		})

		if err != nil {
			return nil, err
		}
		client.Consumer = consumer
	}

	if c.Producer {
		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": c.Server,
			"client.id":         c.ClientId,
			"security.protocol": c.SecurityProtocol,
			"sasl.mechanism":    c.SaslMechanism,
			"sasl.username":     c.SaslUser,
			"sasl.password":     c.SaslPass,
		})

		if err != nil {
			return nil, err
		}
		client.Producer = producer

		dlqProducer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": c.Server,
			"client.id":         c.ClientId,
			"security.protocol": c.SecurityProtocol,
			"sasl.mechanism":    c.SaslMechanism,
			"sasl.username":     c.SaslUser,
			"sasl.password":     c.SaslPass,
		})

		if err != nil {
			return nil, err
		}
		client.dlqProducer = dlqProducer

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
	topic := *m.TopicPartition.Topic
	message := m.Value
	header := kafka.Header{
		Key:   "topic",
		Value: []byte(topic),
	}
	kMessage := &kafka.Message{
		Headers:        []kafka.Header{header},
		TopicPartition: kafka.TopicPartition{Topic: &k.TopicDLQ, Partition: kafka.PartitionAny},
		Value:          message,
	}
	err := k.dlqProducer.Produce(kMessage, nil)
	if err != nil {
		k.ErrorLogFunction("KAFKA ERROR COULD NOT SEND TO DLQ: " + err.Error())
	}
	k.dlqProducer.Flush(15 * 1000)
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
