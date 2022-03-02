package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

const (
	GetItem    = "GET_ITEM"
	GetItems   = "GET_ITEMS"
	PostItem   = "POST_ITEM"
	DeleteItem = "DELETE_ITEM"

	QueueName = "BRQueue"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	defer ch.Close()

	err = setupQueue(ch)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	err = commandPrompts(ch)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	os.Exit(0)
}

func setupQueue(channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(
		QueueName, // name
		false,     // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return err
	}
	return nil
}

func commandPrompts(channel *amqp.Channel) error {
	var err error

	reader := bufio.NewReader(os.Stdin)

	fmt.Println("BloXroute simple messaging client")
	fmt.Println("Type Number of command you want to execute")
	fmt.Println("-----------------------")
	fmt.Println("1. GET ITEM")
	fmt.Println("2. GET ALL ITEMS")
	fmt.Println("3. CREATE ITEM")
	fmt.Println("4. DELETE ITEM")
	fmt.Println("5. EXIT CLIENT")

	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)

		if strings.Compare("1", text) == 0 {
			message := promptNextQuestion(reader, "GET")
			err = sendToQueue(channel, GetItem, message)
			if err != nil {
				err = fmt.Errorf("error when sending to GETItem queue %v", err)
				return err
			}
		} else if strings.Compare("2", text) == 0 {
			fmt.Println("GET ALL ITEMS")
			err = sendToQueue(channel, GetItems, "")
			if err != nil {
				err = fmt.Errorf("error when sending to GETItems queue %v", err)
				return err
			}
		} else if strings.Compare("3", text) == 0 {
			message := promptNextQuestion(reader, "CREATE")
			err = sendToQueue(channel, PostItem, message)
			if err != nil {
				err = fmt.Errorf("error when sending to POSTItem queue %v", err)
				return err
			}
		} else if strings.Compare("4", text) == 0 {
			message := promptNextQuestion(reader, "DELETE")
			err = sendToQueue(channel, DeleteItem, message)
			if err != nil {
				err = fmt.Errorf("error when sending to DELETEItem queue %v", err)
				return err
			}
		} else if strings.Compare("5", text) == 0 {
			fmt.Println("Exiting Client")
			return nil
		} else {
			fmt.Println("Invalid Entry, choose another number")
		}
	}
}

func promptNextQuestion(reader *bufio.Reader, command string) string {
	fmt.Printf("%v ITEM \n", command)
	fmt.Printf("Enter data that you want to %v \n", command)
	message, _ := reader.ReadString('\n')
	message = strings.Replace(message, "\n", "", -1)
	return message
}

func sendToQueue(channel *amqp.Channel, command string, message string) error {
	msg := MQMessage{
		Data:    message,
		Command: command,
	}
	byteData, _ := json.Marshal(msg)

	err := channel.Publish(
		"",
		QueueName, // POSTItem, GETItem, GETItems, DELETEItems
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        byteData,
		},
	)
	if err != nil {
		err = fmt.Errorf("error sending message to queue, queue %v, command %v, message %v", QueueName, command, message)
		return err
	}
	fmt.Println("Message Succesfully Sent")
	return nil
}

type MQMessage struct {
	Data    string `json:"data"`
	Command string `json:"command"`
}
