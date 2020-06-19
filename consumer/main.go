package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	consumerName  = flag.String("consumerid", "sfo-consumer", "name of consumer id in logs")
	brokerList    = flag.String("brokers", "localhost:9092", "The comma separated list of brokers in the Kafka cluster")
	topic         = flag.String("topic", "sanfrancisco", "The topic to consume")
	verbose       = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	logger        = log.New(os.Stderr, "", log.LstdFlags)
	shutdown      = make(chan os.Signal, 1)
	consumergroup sarama.ConsumerGroup
)

func main() {

	var err error

	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.ClientID = *consumerName

	// Start with a client
	client, err := sarama.NewClient(strings.Split(*brokerList, ","), config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Start a new consumer group
	consumergroup, err = sarama.NewConsumerGroupFromClient(fmt.Sprintf("%s-group", *consumerName), client)
	if err != nil {
		panic(err)
	}
	defer consumergroup.Close()

	go ProcessingLoop()
	signal.Notify(shutdown, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-shutdown
	logger.Println("Stopping")

}

func ProcessingLoop() {
	logger.Println("ProcessingLoop")
	ctx := context.Background()

	for {
		handler := &KafkaConsumerGroupHandler{}
		err := consumergroup.Consume(ctx, []string{*topic}, handler)
		if err != nil {
			logger.Println("ProcessingLoop err:", err)
			break
		} else {
			logger.Println("Consume no err")
		}
	}

	logger.Println("Exiting ProcessingLoop()")
}

type KafkaConsumerGroupHandler struct{}

func (KafkaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	logger.Println("Seupt")
	return nil
}
func (KafkaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	logger.Println("Cleanup")
	return nil
}
func (h KafkaConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger.Println("ConsumeClaim")
	for msg := range claim.Messages() {

		if msg == nil {
			break
		}

		key := msg.Key
		if key == nil {
			key = []byte("RESERVED")
		}

		// ensure that we commit this offset so we can resume in case of a crash
		sess.MarkMessage(msg, "")

		err := h.ProcessMessage(key, msg.Offset, msg.Value)
		if err != nil {
			logger.Println("Error consuming: ", err)
			break
		}
	}
	return nil
}

func (h KafkaConsumerGroupHandler) ProcessMessage(key []byte, ofs int64, buf []byte) error {
	logger.Println("Offset: ", ofs)
	return nil
}
