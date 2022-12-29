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
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT, syscall.SIGHUP)

	ctx, cancel := context.WithCancel(context.Background())

	consumers := 1

	for i := 0; i < consumers; i++ {
		go startConsuming(ctx, i)
	}

	<-signals
	cancel()
}

func startConsuming(ctx context.Context, idx int) {
	tracer, err := apm.NewTracer("consumer", "1.0.0")
	if err != nil {
		log.Fatalf("could not create tracer: %s", err.Error())
	}

	consumerGroup := fmt.Sprintf("consumer-group-%d", idx%2)
	log.Printf("starting consumer %d in group %s", idx, consumerGroup)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: consumerGroup,
		Topic:   "kafka-go",
	})
	defer r.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Printf("could not read message: %s", err.Error())
				continue
			}

			traceContext, err := apmTraceContext(m.Headers)
			if err != nil {
				log.Printf("could not parse trace headers: %s", err.Error())
				continue
			}

			tx := tracer.StartTransactionOptions(
				"consuming",
				"messaging",
				apm.TransactionOptions{TraceContext: *traceContext},
			)
			log.Printf("msg %s consumed by %d in group %s\n", string(m.Key), idx, consumerGroup)

			tx.Result = "success"
			tx.End()
		}
	}
}

func apmTraceContext(kafkaHeaders []kafka.Header) (*apm.TraceContext, error) {
	headers := make([]Header, len(kafkaHeaders))
	for _, header := range kafkaHeaders {
		headers = append(headers, Header(header))
	}

	return ParseTraceHeaders(headers...)
}
