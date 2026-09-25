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
		log.Fatalf("Error subscribing to ch", err)
	}
	for {
		inputs := gamelogic.GetInput()
		switch inputs[0] {
		case "spawn":
			err := gamestate.CommandSpawn(inputs)
			if err != nil {
				log.Println("Failed to spawn", err)
			}
		case "move":
			_, err := gamestate.CommandMove(inputs)
			if err != nil {
				log.Println("Failed to move", err)
			} else {
				log.Println("Move has been made")
			}
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

	fmt.Println("RabbitMQ connection closed.")

}
