package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.elastic.co/apm/module/apmhttp/v2"
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
	tracer.SetContinuationStrategy("continue")
	if err != nil {
		log.Fatalf("could not create tracer: %s", err.Error())
	}

	consumerGroup := fmt.Sprintf("consumer-group-%d", idx%2)
	log.Printf("starting consumer %d in group %s", idx, consumerGroup)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: consumerGroup,
		Topic:   "kafka-go",
		//MinBytes: 10e3, // 10KB
		//MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				continue
			}

			var traceParent kafka.Header
			var traceState kafka.Header
			for _, h := range m.Headers {
				if h.Key == "traceparent" {
					traceParent = h
				}
				if h.Key == "tracestate" {
					traceState = h
				}
			}

			txCtx, err := apmhttp.ParseTraceparentHeader(string(traceParent.Value))
			if err != nil {
				fmt.Printf("could not parse trace parent: %s", err.Error())
			}

			txState, err := apmhttp.ParseTracestateHeader(string(traceState.Value))
			if err != nil {
				fmt.Printf("could not parse trace state: %s", err.Error())
			}

			txCtx.State = txState
			tx := tracer.StartTransactionOptions(
				"consuming",
				"messaging",
				apm.TransactionOptions{TraceContext: txCtx},
			)
			log.Println("trace id ctx: ", tx.TraceContext().Trace)
			log.Printf("msg %s consumed by %d in group %s\n", string(m.Key), idx, consumerGroup)
			//time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

			tx.Result = "success"
			tx.End()
		}
	}
}
