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

	armyQueue := fmt.Sprintf("army_moves.%s", userName)
	armyKey := "army_moves.*"
	armyCh, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, armyQueue, armyKey, pubsub.TransientQueue)
	defer armyCh.Close()
	if err != nil {
		log.Fatalf("Error subscribing to army channel", err)
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, armyQueue, armyKey, pubsub.TransientQueue, handlerArmyMove(gamestate))

	if err != nil {
		log.Fatalf("Error subscribing to moves", err)
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
			move, err := gamestate.CommandMove(inputs)
			if err != nil {
				log.Println("Failed to move", err)
			} else {

				pubsub.PublishJSON(armyCh, routing.ExchangePerilTopic, armyKey, move)
				log.Println("Move has been published")
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
}
