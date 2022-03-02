package main

import (
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

const (
	GetItem    = "GET_ITEM"
	GetItems   = "GET_ITEMS"
	PostItem   = "POST_ITEM"
	DeleteItem = "DELETE_ITEM"

	QueueName = "BRQueue"
)

var ItemList []string

type MQMessage struct {
	Data    string `json:"data"`
	Command string `json:"command"`
}

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
	consumeQueue(ch)
}

func consumeQueue(channel *amqp.Channel) {
	msgs, err := channel.Consume(
		QueueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Printf("error in consumeQueue at Consume() %v \n", err)
	}
	fmt.Println("Started Listening to Rabbit MQ GETItem Queue")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			response := MQMessage{}
			err = json.Unmarshal(d.Body, &response)
			if err != nil {
				fmt.Printf("error unmarshalling message %v \n", d.Body)
			} else {
				fmt.Printf("Receied Message %v \n", response)
				err = processMessage(&response)
			}
		}
	}()
	<-forever
}

func processMessage(message *MQMessage) error {
	command := message.Command
	data := message.Data

	switch command {
	case GetItem:
		found := findInItemList(data)
		if found {
			fmt.Printf("%v Found in ItemList", data)
		} else {
			fmt.Printf("%v Not found in ItemList", data)
		}
	case GetItems:
		fmt.Println("Current ItemList")
		fmt.Println(ItemList)
	case PostItem:
		ItemList = append(ItemList, data)
		fmt.Println("Current ItemList")
		fmt.Println(ItemList)
	case DeleteItem:
		removeFromItemList(data)
		fmt.Println("Current ItemList")
		fmt.Println(ItemList)
	}
	return nil
}

func removeFromItemList(data string) {
	for i, v := range ItemList {
		if v == data {
			ItemList = append(ItemList[:i], ItemList[i+1:]...)
		}
	}
}

func findInItemList(data string) bool {
	for i := range ItemList {
		if ItemList[i] == data {
			return true
		}
	}
	return false
}
