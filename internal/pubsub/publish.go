package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	valBytes, err := json.Marshal(val)

	if err != nil {
		log.Println("Failed to marshal json body")
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        valBytes,
	})

	if err != nil {
		log.Println("Failed to publishwithcontext on ch, %v", err)
	}

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var valBytes bytes.Buffer
	enc := gob.NewEncoder(&valBytes)
	err := enc.Encode(val)

	if err != nil {
		log.Println("Failed to encode to Gob")
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        valBytes.Bytes(),
	})

	if err != nil {
		log.Println("Failed to publishwithcontext on ch, %v", err)
	}

	return nil
}
