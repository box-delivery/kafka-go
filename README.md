# kafka-go
Wrapper of the kafka client implementing retry and DLQ

### Usage

```golang
import "fmt"

func LogFunc(l string) {
	fmt.Println(l)
}

func LogErrorFunc(l string) {
	fmt.Println(l)
}

func main() {
	//sample config
	myConfig := &kafkago.KafkaConfig{
		producer:         true,
		consumer:         true,
		server:           "address",
		groupId:          "gid",
		clientId:         "cid",
		autoOffsetReset:  "latest",
		securityProtocol: "SASL_SSL",
		saslMechanism:    "PLAIN",
		saslUser:         "user",
		saslPass:         "pass",
		serviceDLQ:       "mysqServiceDLQ",
	}
	//create the client
	client, err := kafkago.NewClient(myConfig, LogFunc, LogErrorFunc)
	if err != nil {
		//treat it
	}
	//sample topics => channels
	myChannel := make(chan []byte)
	myTopicsAndChannels := map[string]chan []byte{
		"topic1": myChannel,
	}
	//consume
	client.Consume(myTopicsAndChannels)

	//produce
	client.Produce("topic1", []byte("My message"))
}
```