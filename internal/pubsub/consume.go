package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	DurableQueue SimpleQueueType = iota
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

	q, err := ch.QueueDeclare(queueName,
		queueType == DurableQueue,
		queueType != DurableQueue,
		queueType != DurableQueue,
		false,
		nil)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, q, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("It was not possible to bind queue %v", err)
	}

	consumerCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("It was not possible to create channel %v", err)
	}
	go func() {
		for val := range consumerCh {
			var jBody T
			err := json.Unmarshal(val.Body, &jBody)
			if err != nil {
				log.Println("It was not possible  to unmarshal in go routine", err)
			}

			handler(jBody)

			val.Ack(false)
		}
	}()

	return nil
}
