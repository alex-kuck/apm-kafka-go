package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.elastic.co/apm/v2"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	producers := 1

	for i := 0; i < producers; i++ {
		go startProducing(ctx, i)
	}

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT, syscall.SIGHUP)

	<-signals
	cancel()
}

var txResults = []string{"success", "failure"}

func startProducing(ctx context.Context, idx int) {
	tracer, err := apm.NewTracer("producer", "1.0.0")
	if err != nil {
		log.Fatalf("could not create tracer: %s", err.Error())
	}

	log.Printf("Start producer %d", idx)
	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "kafka-go",
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	msg := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			tx := tracer.StartTransaction("producing", "scheduled")
			ctx = apm.ContextWithTransaction(ctx, tx)

			kafkaHeaders, err := apmTraceHeaders(ctx)
			if err != nil {
				log.Printf("could not extract apm trace headers: %s", err.Error())
			}

			err = w.WriteMessages(ctx, kafka.Message{
				Key:     []byte(fmt.Sprintf("producer-%d-msg-%d", idx, msg)),
				Headers: kafkaHeaders,
			})
			if err != nil {
				log.Printf("got error while sending from producer %d: %s", idx, err.Error())
			}

			msg++
			tx.Result = txResults[msg%2]
			tx.End()
		}
	}
}

func apmTraceHeaders(ctx context.Context) ([]kafka.Header, error) {
	headers, err := TraceHeaders(ctx)
	if err != nil {
		return nil, err
	}

	kafkaHeaders := make([]kafka.Header, len(headers))
	for _, header := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header(header))
	}
	return kafkaHeaders, nil
}
