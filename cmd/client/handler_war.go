package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerWar(ch *amqp.Channel, gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			fallthrough
		case gamelogic.WarOutcomeYouWon:
			msgLog := fmt.Sprintf("%s won a war agasint %s", winner, loser)
			err := logGame(ch, msgLog, gs.GetUsername())
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			msgLog := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := logGame(ch, msgLog, gs.GetUsername())
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		default:
			fmt.Println("Unknown outcome of war")
			return pubsub.NackDiscard
		}
	}
}
