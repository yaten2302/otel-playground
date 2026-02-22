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

func readFromKafka(ctx context.Context) ([]OrderEvent, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders.created",
	})
	defer reader.Close()

	var events []OrderEvent

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("Kafka read error:", err)
			return nil, err
		}

		var event OrderEvent
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Println("JSON unmarshal error:", err)
			continue
		}

		events = append(events, event)
	}
}
