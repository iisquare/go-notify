package canal

import (
	"github.com/CanalClient/canal-go/client"
	protocol "github.com/CanalClient/canal-go/protocol"
	"os"
	"os/signal"
	"time"
)

type ChanItem struct {
	Connector *client.SimpleCanalConnector
	Message *protocol.Message
}

type Client struct {
	connectors []*client.SimpleCanalConnector
	Destinations []string
	Address           string
	Port              int
	UserName          string
	PassWord          string
	BatchSize int32
}

func (this *Client) Connect() error {
	var connectors []*client.SimpleCanalConnector
	for _,destination := range this.Destinations {
		connector := client.NewSimpleCanalConnector(this.Address, this.Port, this.UserName, this.PassWord, destination, 60000, 60*60*1000)
		connector.Connect()
		connector.Subscribe(".*\\\\..*")
		connectors = append(connectors, connector)
	}
	this.connectors = connectors
	return nil
}

func (this *Client) DisConnection()  {
	for _, connector := range this.connectors {
		connector.DisConnection()
	}
	this.connectors = nil
}

func (this *Client) get(connector *client.SimpleCanalConnector, channel chan *ChanItem) {
	for {
		message := connector.GetWithOutAck(this.BatchSize, nil, nil)
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(300 * time.Millisecond)
			connector.Ack(batchId)
			continue
		}
		channel <- &ChanItem{
			Connector: connector,
			Message: message,
		}
	}
}

func (this *Client) OnMessage(callback func(message *protocol.Message) bool) {
	channel := make(chan *ChanItem)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)
	for _, connector := range this.connectors {
		go this.get(connector, channel)
	}
	msg: for {
		select {
		case item := <- channel:
			if callback(item.Message) {
				item.Connector.Ack(item.Message.Id)
			} else {
				item.Connector.RollBack(item.Message.Id)
			}
			break
		case <-signals:
			break msg
		}
	}
}
