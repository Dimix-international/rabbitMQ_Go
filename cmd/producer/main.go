package main

import (
	"context"
	"log"
	"time"

	"github.com/Dimix-international/rabbitMQ_Go/internal/rabbitMq"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	clientRabbit, err := rabbitMq.NewRabbitClient("dima", "qwerty", "localhost:5672", "customers")
	if err != nil {
		log.Panicf("%s", err)
	}

	defer clientRabbit.Close()

	if err := clientRabbit.CreateQueue("customers_created", true, false); err != nil {
		log.Panicf("%s", err)
	}

	if err := clientRabbit.CreateQueue("customers_test", false, true); err != nil {
		log.Panicf("%s", err)
	}

	if err := clientRabbit.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
		log.Panicf("%s", err)
	}

	if err := clientRabbit.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
		log.Panicf("%s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := clientRabbit.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Persistent, //сохранится сообщение в случае остановки сервера
		Body:         []byte(`An cool message between services`),
	}); err != nil {
		log.Panicf("%s", err)
	}

	if err := clientRabbit.Send(ctx, "customer_events", "customers.test", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Transient,
		Body:         []byte(`An uncool undurable message`),
	}); err != nil {
		log.Panicf("%s", err)
	}

	time.Sleep(10 * time.Second)
}
