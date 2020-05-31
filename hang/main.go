package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	scluster "github.com/bsm/sarama-cluster"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	consumerName = flag.String("consumerid", "sfo-consumer", "name of consumer id in logs")
	brokerList   = flag.String("brokers", "localhost:9092", "The comma separated list of brokers in the Kafka cluster")
	topic        = flag.String("topic", "sanfrancisco", "The topic to consume")
	verbose      = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	logger       = log.New(os.Stderr, "", log.LstdFlags)
	shutdown     = make(chan os.Signal, 1)
	consumer     *scluster.Consumer
)

func main() {

	var err error

	cfg := scluster.NewConfig()
	cfg.Version = sarama.V2_2_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Return.Errors = true
	cfg.Group.Return.Notifications = true
	cfg.ClientID = *consumerName

	consumer, err = scluster.NewConsumer(strings.Split(*brokerList, ","),
		fmt.Sprintf("%s-consumer", *consumerName),
		[]string{*topic}, cfg)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	go ProcessingLoop()
	signal.Notify(shutdown, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-shutdown
	logger.Println("Stopping")
}

// ProcessingLoop reads message batches from kafka and inserts them
// to processing queues for individual systems.
func ProcessingLoop() {
	logger.Println("ProcessingLoop")
	for {
		_, ofs, evtbatch, _ := BatchRead()

		if evtbatch == nil {
			logger.Println("Done processing")
			break
		} else {
			if len(evtbatch) == 0 {
				continue
			}
		}

		logger.Println("Offset ", ofs)
	}

	logger.Println("ProcessingLoop closing")
}

func BatchRead() (key []byte, ofs int64, buf []byte, err error) {
	logger.Println("BatchRead")
	msg := <-consumer.Messages()
	if msg == nil {
		return nil, 0, nil, nil
	}
	key = msg.Key
	if key == nil {
		key = []byte("RESERVED")
	}
	// BUG:
	consumer.MarkOffset(msg, "")
	return key, msg.Offset, msg.Value, nil
}

