package bus

import (
	"encoding/json"
	"time"

	nats "github.com/nats-io/go-nats"
)

type Bus struct {
	u *[]string
	c *nats.Conn
}

func New(urls *[]string) *Bus {
	bus := &Bus{u: urls}
	return bus
}

func (bus *Bus) Connect() error {
	var err error
	for _, url := range *bus.u {
		bus.c, err = nats.Connect(url)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (bus *Bus) Request(topic string, req interface{}, resp interface{}) error {
	indata, _ := json.Marshal(req)
	outmsg, err := bus.c.Request(topic, indata, 1000*time.Millisecond)
	if err != nil {
		err = bus.Connect()
		if err != nil {
			return err
		}
		outmsg, _ = bus.c.Request(topic, indata, 1000*time.Millisecond)
	}
	json.Unmarshal(outmsg.Data, resp)
	return nil
}

func (bus *Bus) Connection() *nats.Conn {
	return bus.c
}
