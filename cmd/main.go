package main

import (
	"log"
	"time"

	rabbitmq "github.com/Dimix-international/rabbitMQ_Go/internal/rabbitMq"
)

func main() {
	clientRabbit, err := rabbitmq.NewRabbitClient("dima", "qwerty", "localhost:5672", "customers")
	if err != nil {
		log.Panicf("%s", err)
	}

	defer clientRabbit.Close()
	time.Sleep(10 * time.Second)
}
