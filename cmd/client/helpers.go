package main

import (
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func logGame(ch *amqp.Channel, message, userName string) error {

	gl := routing.GameLog{
		Message:     message,
		Username:    userName,
		CurrentTime: time.Now(),
	}
	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+userName, gl)
}
