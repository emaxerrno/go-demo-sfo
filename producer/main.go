package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var (
	groupId       = flag.String("groupid", "sfo", "name of group id in logs")
	brokerList    = flag.String("brokers", "localhost:9092", "The comma separated list of brokers in the Kafka cluster")
	topic         = flag.String("topic", "sanfrancisco", "The topic to consume")
	verbose       = flag.Bool("verbose", true, "Whether to turn on sarama logging")
	logger        = log.New(os.Stderr, "", log.LstdFlags)
	shutdown      = make(chan os.Signal, 1)
	consumergroup sarama.ConsumerGroup
	syncproducer  sarama.SyncProducer
)

func main() {

	var err error

	syncproducer, err = newSyncProducer(strings.Split(*brokerList, ","))
	if err != nil {
		panic(err)
	}
	defer syncproducer.Close()

	go SendingLoop()
	signal.Notify(shutdown, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-shutdown
	logger.Println("Stopping")

}

func newSyncProducer(brokerList []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal // Wait for just the one replica to ack the message
	config.Producer.Retry.Max = 10                     // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_2_0_0

	return sarama.NewSyncProducer(brokerList, config)
}

func SendingLoop() {
	logger.Println("SendingLoop")

	for {
		partition, offset, err := syncproducer.SendMessage(&sarama.ProducerMessage{
			Topic: *topic,
			Key:   sarama.ByteEncoder([]byte{0x0}),
			Value: sarama.ByteEncoder([]byte{0x0}),
		})
		logger.Printf("Sending to partition %d at offset %d\n", partition, offset)
		if err != nil {
			fmt.Printf("Couldn't send kafka event channel message with unique identifier /%d/%d, error:, %s", partition, offset, err)
		}

		time.Sleep(time.Second)

	}

	logger.Println("Exiting SendingLoop()")
}
