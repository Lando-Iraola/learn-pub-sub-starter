package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	conString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(conString)
	if err != nil {
		slog.Error("Failed to connect to rabbitmq", err)
	}
	defer conn.Close()

	fmt.Println("Connected to rabbitmq")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	interrupted := <-signalChan
	fmt.Printf("%v Signal, shutting down\n", interrupted)
	os.Exit(0)
}
