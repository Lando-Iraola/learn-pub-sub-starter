package pubsub

import (
	"context"
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

type SimpleQueueType int

const (
	DurableQueue SimpleQueueType = 1
	TransientQueue
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	isDurable := false
	if queueType == DurableQueue {
		isDurable = true
	}

	isAutoDelete := false
	isExclusive := false
	if queueType == TransientQueue {
		isAutoDelete = true
		isExclusive = true
	}

	q, err := ch.QueueDeclare(queueName, isDurable, isAutoDelete, isExclusive, false, nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, q, nil
}
