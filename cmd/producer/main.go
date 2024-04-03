package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Dimix-international/rabbitMQ_Go/internal/rabbitMq"
	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	// clientRabbit, err := rabbitMq.NewRabbitClient("dima", "qwerty", "localhost:5672", "customers")
	// if err != nil {
	// 	log.Panicf("%s", err)
	// }

	// defer clientRabbit.Close()
	//example with exchange topic

	// if err := clientRabbit.CreateQueue("customers_created", true, false); err != nil {
	// 	log.Panicf("%s", err)
	// }

	// if err := clientRabbit.CreateQueue("customers_test", false, true); err != nil {
	// 	log.Panicf("%s", err)
	// }

	// if err := clientRabbit.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
	// 	log.Panicf("%s", err)
	// }

	// if err := clientRabbit.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
	// 	log.Panicf("%s", err)
	// }

	//example with exchange fanout

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()

	// if err := clientRabbit.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
	// 	ContentType:  "text/plain",
	// 	DeliveryMode: amqp091.Persistent, //сохранится сообщение в случае остановки сервера
	// 	Body:         []byte(`An cool message between services`),
	// }); err != nil {
	// 	log.Panicf("%s", err)
	// }

	// if err := clientRabbit.Send(ctx, "customer_events", "customers.test", amqp091.Publishing{
	// 	ContentType:  "text/plain",
	// 	DeliveryMode: amqp091.Transient,
	// 	Body:         []byte(`An uncool undurable message`),
	// }); err != nil {
	// 	log.Panicf("%s", err)
	// }

	//пример что ожидаем ответ от consumer. - коннект для чтения
	//нелья делать connection для получения и отправки сообщений в одном сервисе

	produceClientRabbit, err := rabbitMq.NewRabbitClient("dima", "qwerty", "localhost:5672", "customers")
	if err != nil {
		log.Panicf("%s", err)
	}

	defer produceClientRabbit.Close()

	consumeClientRabbit, err := rabbitMq.NewRabbitClient("dima", "qwerty", "localhost:5672", "customers")
	if err != nil {
		log.Panicf("%s", err)
	}
	defer consumeClientRabbit.Close()

	queue, err := consumeClientRabbit.CreateQueueWithReturnQueue("", true, true)
	if err != nil {
		panic(err)
	}

	//binding to direct exchange
	if err := consumeClientRabbit.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		if err := produceClientRabbit.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp091.Persistent, //сохранится сообщение в случае остановки сервера
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprintf("customer id: %d", i),
			Body:          []byte(`An cool message between services`),
		}); err != nil {
			log.Panicf("%s", err)
		}
	}

	messageBus, err := consumeClientRabbit.Consume(queue.Name, "customer-api", true)
	if err != nil {
		log.Panicf("%s", err)
	}

	var block chan struct{}
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	g, _ := errgroup.WithContext(ctxTimeout)

	g.SetLimit(10)

	go func() {
		for message := range messageBus {

			msg := message ///spawn a worker

			g.Go(func() error {
				log.Printf("message: %v, %v", msg.CorrelationId, string(msg.Body))
				if err := msg.Ack(false); err != nil {
					return err
				}

				return nil
			})
		}
	}()

	<-block

}
