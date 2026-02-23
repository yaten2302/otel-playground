package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

var writer = &kafka.Writer{
	Addr:                   kafka.TCP("localhost:9092"),
	Topic:                  "orders.created",
	Balancer:               &kafka.LeastBytes{},
	AllowAutoTopicCreation: true,
}

func publishToKafka(ctx context.Context, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Value: data,
	}

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Println("Kafka write error:", err)
		return err
	}

	return nil
}

func startKafkaConsumer(ctx context.Context) {
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
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("Kafka read error:", err)
			continue
		}

		var event OrderEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("JSON unmarshal error:", err)
			continue
		}

		_, err = db.Exec(
			"INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES ($1,$2,$3,$4)",
			event.OrderID,
			event.CustomerID,
			event.Amount,
			event.CreatedAt,
		)
		if err != nil {
			log.Println("Database insert error:", err)
			continue
		}

		log.Println("Order inserted:", event.OrderID)
	}
}
