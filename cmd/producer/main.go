package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT, syscall.SIGHUP)

	var enqueued, producerErrors int
ProducerLoop:
	for {
		var buf bytes.Buffer
		gob.NewEncoder(&buf).Encode(map[string]string{"hello": "world"})
		msg := &sarama.ProducerMessage{
			Topic: "my_topic",
			Key:   sarama.StringEncoder(fmt.Sprintf("msg-%d", enqueued)),
			Value: sarama.ByteEncoder(buf.Bytes()),
		}
		select {
		case producer.Input() <- msg:
			enqueued++
			time.Sleep(250 * time.Millisecond)
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}
