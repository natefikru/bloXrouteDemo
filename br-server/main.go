package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/streadway/amqp"
)

const (
	GetItem    = "GET_ITEM"
	GetItems   = "GET_ITEMS"
	PostItem   = "POST_ITEM"
	DeleteItem = "DELETE_ITEM"

	QueueName  = "BRQueue"
	ValuesFile = "values.log"
)

var ItemList []string

type MQMessage struct {
	Data    string `json:"data"`
	Command string `json:"command"`
}

type BRServer struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Deliveries <-chan amqp.Delivery
	File       *os.File
}

func newBRServer() *BRServer {
	return &BRServer{}
}

func main() {
	brServer := newBRServer()
	for i := 1; i < 5; i++ {
		brServer.run()
	}
}

func (s *BRServer) run() {
	err := s.initFileConn()
	if err != nil {
		s.print(err.Error())
		panic(err)
	}

	s.print(fmt.Sprintf("Server is starting at %v: ", time.Now().String()))

	err = s.initMQConn()
	if err != nil {
		s.print(err.Error())
		panic(err)
	}

	s.print("Initialized MQ connection")

	defer s.Connection.Close()
	defer s.Channel.Close()
	defer s.File.Close()

	err = s.getDeliveries()
	if err != nil {
		s.print(err.Error())
		panic(err)
	}
	s.print("Initialized MQ Deliveries")

	err = s.consumeQueue()
	if err != nil {
		s.print(err.Error())
		panic(err)
	}
}

func (s *BRServer) initMQConn() error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	s.Connection = conn
	s.Channel = channel

	return nil
}

func (s *BRServer) initFileConn() error {
	var f *os.File
	exists, err := valueFileExists()
	if err != nil {
		return err
	}
	if !exists {
		f, err = os.Create(ValuesFile)
		if err != nil {
			return err
		}

	} else {
		f, err = os.OpenFile(ValuesFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	}
	s.File = f
	return nil
}

func (s *BRServer) getDeliveries() error {
	deliveries, err := s.Channel.Consume(QueueName, "", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error in consumeQueue at postItemMessages Consume() %v", err)
	}

	s.Deliveries = deliveries
	return nil
}

func (s *BRServer) consumeQueue() error {
	s.print("Starting RabbitMQ Consumer Process")

	done := make(chan bool)
	go func() {
		for msg := range s.Deliveries {
			s.processMessage(msg.Body)
		}
	}()
	<-done
	return nil
}

func (s *BRServer) processMessage(body []byte) {
	message := MQMessage{}
	err := json.Unmarshal(body, &message)
	if err != nil {
		s.print(fmt.Sprintf("error unmarshalling message: %v %v", body, err))
	} else {
		s.print(fmt.Sprintf("Received Message %v", message))
	}

	data := message.Data
	command := message.Command
	switch command {
	case GetItem:
		found := findInItemList(data)
		if found {
			s.print(fmt.Sprintf("%v Found in ItemList", data))
		} else {
			s.print(fmt.Sprintf("%v Not found in ItemList", data))
		}
	case GetItems:
		s.print(fmt.Sprintf("Current ItemList: %v", ItemList))
	case PostItem:
		ItemList = append(ItemList, data)
		// _, err := s.File.WriteString(data + "\n")
		// if err != nil {
		// 	s.print(err.Error())
		// }
		s.print(fmt.Sprintf("Added Item: %v", data))
		s.print(fmt.Sprintf("Current ItemList: %v", ItemList))
	case DeleteItem:
		removeFromItemList(data)
		s.print(fmt.Sprintf("Removed Item: %v", data))
		s.print(fmt.Sprintf("Current ItemList: %v", ItemList))
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

func valueFileExists() (bool, error) {
	_, err := os.Stat(ValuesFile)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (s *BRServer) print(str string) {
	fmt.Fprintln(s.File, str)
	fmt.Println(str)
}
