package rabbitMq

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	conn *amqp.Connection //used by client
	ch   *amqp.Channel    //send messages
}

func NewRabbitClient(username, password, host, vhost string) (*RabbitClient, error) {
	connect, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
	if err != nil {
		return nil, err
	}

	channel, err := connect.Channel()
	if err != nil {
		return nil, err
	}

	if err := channel.Confirm(false); err != nil {
		//Подтверждение переводит этот канал в режим подтверждения, чтобы клиент мог убедиться, что все публикации успешно получены сервером.
		return nil, err
	}

	return &RabbitClient{conn: connect, ch: channel}, nil
}

// CreateQueue will create a new queue based on given cfgs - exchange topic
func (r *RabbitClient) CreateQueue(queueName string, durable, autodelete bool) error {
	_, err := r.ch.QueueDeclare(queueName, durable, autodelete, false, false, amqp.Table{})
	return err
}

func (r *RabbitClient) CreateQueueWithReturnQueue(queueName string, durable, autodelete bool) (amqp.Queue, error) {
	q, err := r.ch.QueueDeclare(queueName, durable, autodelete, false, false, amqp.Table{})
	if err != nil {
		return amqp.Queue{}, nil
	}
	return q, nil
}

// CreateBinding will bind the current channel to the given exchange using the routing key provided
func (r *RabbitClient) CreateBinding(queueName string, bindingKey, exchangeName string) error {
	//leaving nowait false - make the channel return error if it is fails
	return r.ch.QueueBind(queueName, bindingKey, exchangeName, false, amqp.Table{})
}

// Send is used to publish payloads into an echange with the given routing key
func (r *RabbitClient) Send(ctx context.Context, exchangeName string, routingKey string, options amqp.Publishing) error {
	//mandatory  - is used to determine if an error should be returned upon failure
	//immediate - false - is deprecated
	//return r.ch.PublishWithContext(ctx, exchangeName, routingKey, true, false, options)

	//нужно для канала установить confirm mode - channel.Confirm(false)
	//позволяя вызывающей стороне дождаться подтверждения этого сообщения от издателя - статут отправки
	confirmation, err := r.ch.PublishWithDeferredConfirmWithContext(ctx, exchangeName, routingKey, true, false, options)
	if err != nil {
		return err
	}

	log.Println(confirmation.Wait()) //Блоки ожидания до получения подтверждения от издателя. Возвращает значение true, если сервер успешно получил публикацию
	return nil
}

// Consume is used to consume a queue
func (r *RabbitClient) Consume(queueName, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	//autoAck - отвечать на ACK автоматически, true - да, ACK ответить означает, что я получил сообщение от высокоскоростного сервера
	//exclusive - текущая очередь может использоваться только одним потребителем
	//noLocal - true - производитель и потребитель не могут быть одним и тем же соединением
	//nowait - true  - после отправки запроса на создание коммутатора он блокируется до тех пор, пока сервер RMQ не вернет информацию
	return r.ch.Consume(queueName, consumer, autoAck, false, false, false, amqp.Table{})
}

// ApplyQos - Qos определяет, сколько сообщений или сколько байт сервер попытается сохранить в сети для пользователей, прежде чем получит подтверждение о доставке.
//
//	Цель Qos - обеспечить заполнение сетевых буферов между сервером и клиентом
//
// prefetch count - an integer on how many uncknowledge messages the server can send
// prefetch size - is int how many bytes
// global - determines of the rule should be applied globally or not
func (r *RabbitClient) ApplyQos(count, size int, global bool) error {
	return r.ch.Qos(count, size, global)
}

func (r *RabbitClient) Close() {
	r.ch.Close()
	r.conn.Close()
}
