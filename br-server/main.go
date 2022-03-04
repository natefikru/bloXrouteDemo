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
	err = consumeQueue(ch)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

}

func consumeQueue(channel *amqp.Channel) error {
	messages, err := channel.Consume(QueueName, "", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error in consumeQueue at postItemMessages Consume() %v", err)
	}
	fmt.Println("Started RabbitMQ Consumer Process")

	mqChannel := make(chan bool)
	go func() {
		for msg := range messages {
			go processMessage(msg.Body)
		}
	}()
	<-mqChannel
	return nil
}

func processMessage(body []byte) {
	message := MQMessage{}
	err := json.Unmarshal(body, &message)
	if err != nil {
		fmt.Printf("error unmarshalling message: %v %v \n", body, err)
	} else {
		fmt.Printf("Receied Message %v \n", message)
	}
	data := message.Data
	command := message.Command
	switch command {
	case GetItem:
		found := findInItemList(data)
		if found {
			fmt.Printf("%v Found in ItemList \n", data)
		} else {
			fmt.Printf("%v Not found in ItemList\n", data)
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
