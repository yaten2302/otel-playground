package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var writer = &kafka.Writer{
	Addr:                   kafka.TCP("localhost:9092"),
	Topic:                  "orders.created",
	Balancer:               &kafka.LeastBytes{},
	AllowAutoTopicCreation: true,
}

var tracer = otel.Tracer("orders-kafka")

func publishToKafka(ctx context.Context, event any) error {
	ctx, span := tracer.Start(ctx, "kafka.produce")
	defer span.End()

	data, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		return err
	}

	msg := kafka.Message{
		Value: data,
	}

	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)

	headers := make([]kafka.Header, 0, len(carrier))
	for k, v := range carrier {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	msg.Headers = headers

	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination", "orders"),
	)

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Println("Kafka write error:", err)
		span.RecordError(err)
		return err
	}

	return nil
}

func startKafkaConsumer(parentCtx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders.created",
		GroupID:  "orders-service",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Kafka consumer started")

	for {
		msg, err := reader.ReadMessage(parentCtx)
		if err != nil {
			log.Println("Kafka read error:", err)
			continue
		}

		carrier := propagation.MapCarrier{}
		for _, h := range msg.Headers {
			carrier[h.Key] = string(h.Value)
		}

		ctx := otel.GetTextMapPropagator().Extract(parentCtx, carrier)
		ctx, span := tracer.Start(ctx, "kafka.consume", trace.WithSpanKind(trace.SpanKindConsumer))

		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", "orders.created"),
			attribute.String("messaging.operation", "process"),
			attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		)

		var event OrderEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("JSON unmarshal error:", err)
			span.RecordError(err)
			span.End()
			continue
		}

		dbCtx, dbSpan := tracer.Start(ctx, "postgres.insert")

		_, err = db.ExecContext(
			dbCtx,
			"INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES ($1,$2,$3,$4)",
			event.OrderID,
			event.CustomerID,
			event.Amount,
			event.CreatedAt,
		)
		if err != nil {
			log.Println("Database insert error:", err)
			dbSpan.RecordError(err)
			span.RecordError(err)
			dbSpan.End()
			span.End()
			continue
		}

		dbSpan.End()
		span.End()

		log.Println("Order inserted:", event.OrderID)
	}
}
