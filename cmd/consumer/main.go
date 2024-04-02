package main

import (
	"context"
	"log"
	"time"

	"github.com/Dimix-international/rabbitMQ_Go/internal/rabbitMq"
	"golang.org/x/sync/errgroup"
)

func main() {
	clientRabbit, err := rabbitMq.NewRabbitClient("dima", "qwerty", "localhost:5672", "customers")
	if err != nil {
		log.Panicf("%s", err)
	}

	defer clientRabbit.Close()

	messageBus, err := clientRabbit.Consume("customers_created", "email-service", false)
	if err != nil {
		log.Panicf("%s", err)
	}
	var block chan struct{}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	g, _ := errgroup.WithContext(ctx)

	g.SetLimit(10)

	go func() {
		for message := range messageBus {

			msg := message ///spawn a worker

			g.Go(func() error {
				log.Printf("message: %v", msg)
				//если сообщение не доставлено
				// if !msg.Redelivered {
				// 	//multiple равно true, то сообщения, включая доставленные сообщения, будут перенаправляться до тех пор, пока тег доставки не будет доставлен по тому же каналу.
				// 	//requeue  запросите сервер доставить это сообщение другому пользователю. Если это невозможно или запрос имеет значение false, сообщение будет удалено или отправлено в очередь ожидания
				// 	msg.Nack(false, true)
				// }

				// если autoAck в вызове метода clientRabbit.Consume - true - этот метод не нужно вызывать. но опасно, что можем потерять если будет у нас panic
				// т.к. мы ответили что сообщение получили
				// true - эта доставка и все предыдущие неподтвержденные доставки по тому же каналу будут подтверждены
				if err := msg.Ack(false); err != nil {
					return err
				}

				return nil
			})
		}
	}()

	<-block
}
