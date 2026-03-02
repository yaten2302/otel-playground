package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/baggage"
)

var db *sql.DB

func main() {
	ctx := context.Background()
	var err error

	godotenv.Load(".env")

	shutdown, err := setupOTelSDK(ctx)
	if err != nil {
		log.Fatal("Failed to set up OpenTelemetry:", err)
	}
	defer shutdown(ctx)

	db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	go startKafkaConsumer(ctx)

	handler := http.HandlerFunc(ordersHandler)

	http.Handle("/orders", otelhttp.NewHandler(handler, "create-orders"))

	log.Println("Server running on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func ordersHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		createOrder(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

type CreateOrderRequest struct {
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	CreatedAt  time.Time `json:"created_at"`
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	customerID := r.URL.Query().Get("customer_id")
	amountStr := r.URL.Query().Get("amount")
	failType := r.URL.Query().Get("fail")

	if failType != "" {
		member, _ := baggage.NewMember("fail", failType)
		bg, _ := baggage.New(member)
		ctx = baggage.ContextWithBaggage(ctx, bg)
	}

	if customerID == "" || amountStr == "" {
		http.Error(w, "customer_id and amount are required", http.StatusBadRequest)
		return
	}

	amount, err := strconv.ParseFloat(amountStr, 64)
	if err != nil {
		http.Error(w, "invalid amount", http.StatusBadRequest)
		return
	}

	orderID := uuid.New().String()

	event := OrderEvent{
		OrderID:    orderID,
		CustomerID: customerID,
		Amount:     amount,
		CreatedAt:  time.Now(),
	}

	if failType == FailContext {
		log.Println("Simulating context propagation failure...")
		ctx = context.Background()
	}

	if err := publishToKafka(ctx, event, failType); err != nil {
		http.Error(w, "Failed to publish", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": orderID,
		"status":   "accepted",
	})
}
