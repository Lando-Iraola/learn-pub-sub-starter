package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	conString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(conString)
	if err != nil {
		log.Fatalf("client could not connect to RabbbitMQ: %v", err)
	}
	defer conn.Close()

	userName, err := gamelogic.ClientWelcome()

	if err != nil {
		log.Fatalf("Error at welcome", err)
	}

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, userName)
	gamestate := gamelogic.NewGameState(userName)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.TransientQueue, handlerPause(gamestate))

	if err != nil {
		log.Fatalf("Error subscribing to pause", err)
	}

	armyQueue := routing.ArmyMovesPrefix + "." + userName
	armyKey := routing.ArmyMovesPrefix + ".*"
	armyCh, err := conn.Channel()
	defer armyCh.Close()
	if err != nil {
		log.Fatalf("Error subscribing to army channel", err)
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, armyQueue, armyKey, pubsub.TransientQueue, handlerArmyMove(armyCh, gamestate))

	if err != nil {
		log.Fatalf("Error subscribing to moves", err)
	}

	warQueue := routing.WarRecognitionsPrefix
	warKey := routing.WarRecognitionsPrefix + ".*"
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, warQueue, warKey, pubsub.DurableQueue, handlerWar(armyCh, gamestate))

	if err != nil {
		log.Fatalf("Error subscribing to war", err)
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		switch inputs[0] {
		case "spawn":
			err := gamestate.CommandSpawn(inputs)
			if err != nil {
				log.Println("Failed to spawn", err)
				continue
			}
		case "move":
			move, err := gamestate.CommandMove(inputs)
			if err != nil {
				log.Println("Failed to move", err)
				continue
			}

			err = pubsub.PublishJSON(armyCh, routing.ExchangePerilTopic, armyKey, move)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			log.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)

		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			log.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			log.Printf("%v command not recognized\n", inputs[0])
		}
	}
}
