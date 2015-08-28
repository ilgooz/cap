package cap

//todo: cd style declare/bind

import (
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

var ReconnectDelay = time.Second * 5

var (
	DifferentConnErr = errors.New("connection has been changed")
)

var log = logrus.New()

type Cap struct {
	addr string

	conn     *amqp.Connection
	connReq  chan bool
	waitConn chan chan bool
	m        sync.Mutex

	funcs []func()
}

// Open opens a connection to amqp server with given addr
// and re-connects if the connection drops.
// Returns an error if the addr is invalid
func Open(addr string) (*Cap, error) {
	_, err := amqp.ParseURI(addr)
	if err != nil {
		return nil, err
	}
	cap := &Cap{
		addr:    addr,
		connReq: make(chan bool, 0),
	}
	go cap.connectLoop()
	cap.connect()
	return cap, nil
}

func (c *Cap) connect() {
	go func() { c.connReq <- true }()
}

func (c *Cap) connectLoop() {
	var (
		connect   bool
		connected = make(chan bool, 0)
		waitings  = make([]chan bool, 0)
	)

	for {
		select {
		case <-c.connReq:
			if !connect {
				continue
			}
			connect = false

			go func() {
				cn, err := amqp.Dial(c.addr)
				if err != nil {
					log.Infof("couldn't connect to server err: %s", err)
					time.Sleep(ReconnectDelay)
				}

				go func() {
					err := <-cn.NotifyClose(make(chan *amqp.Error, 0))
					log.Infof("connection lost err: %s", err)
					connected <- false
				}()

				connected <- true
				log.Info("connected")

				c.m.Lock()
				c.conn = cn
				c.m.Unlock()
			}()

		case is := <-connected:
			connect = !is
			if connect {
				c.connect()
			} else {
				c.m.Lock()
				for _, f := range c.funcs {
					f()
				}
				c.m.Unlock()
				for _, reply := range waitings {
					reply <- false
				}
			}

		case reply := <-c.waitConn:
			if !connect {
				reply <- true
			} else {
				waitings = append(waitings, reply)
			}
		}
	}
}

func (c *Cap) getConnReady() bool {
	wait := make(chan bool, 0)
	c.waitConn <- wait
	return <-wait
}

// Always calls given func immediately if the current connection is valid
// and anytime right after a connection made to server.
// Call this func as many as you want to register multiple funcs
func (c *Cap) Always(f func()) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.getConnReady() {
		f()
	} else {
		c.funcs = append(c.funcs, f)
	}
}

type Channel struct {
	*amqp.Channel
	connAddr net.Addr
	cap      *Cap
}

func (c *Cap) newChannel(ch *amqp.Channel) *Channel {
	return &Channel{
		Channel:  ch,
		connAddr: c.conn.LocalAddr(),
		cap:      c,
	}
}

// AlwaysChannel calls given func with a valid channel for the first time
// and anytime after a re-connection made to server
func (c *Cap) AlwaysChannel(f func(*Channel)) {
	ch, err := c.CreateChannel()
	if err != nil {
		c.AlwaysChannel(f)
		return
	}
	go func() {
		<-ch.NotifyClose(make(chan *amqp.Error, 0))
		c.AlwaysChannel(f)
	}()
	f(ch)
}

// CreateChannel creates a channel for the current connection.
// It waits for a valid connection before creating a channel if there is not
func (c *Cap) CreateChannel() (*Channel, error) {
	c.getConnReady()

	c.m.Lock()
	defer c.m.Unlock()

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}

	return c.newChannel(ch), nil
}

// CreateTxChannel creates a channel with Channel() but in transactional mode
func (c *Cap) CreateTxChannel() (*Channel, error) {
	ch, err := c.CreateChannel()
	if err != nil {
		return nil, err
	}
	return ch, ch.Tx()
}

// CreateChannel creates a channel from the same connection of the channel
// or returns an error if the connection is no longer valid
func (ch *Channel) CreateChannel() (*Channel, error) {
	ch.cap.m.Lock()
	defer ch.cap.m.Unlock()

	if ch.cap.conn.LocalAddr() == ch.connAddr {
		chn, err := ch.cap.conn.Channel()
		if err != nil {
			return nil, err
		}
		return ch.cap.newChannel(chn), nil
	}

	return nil, DifferentConnErr
}

// CreateTxChannel creates a channel with Channel.Channel() but in transactional mode
func (ch *Channel) CreateTxChannel() (*Channel, error) {
	chn, err := ch.CreateChannel()
	if err != nil {
		return nil, err
	}
	return chn, chn.Tx()
}

type Delivery struct {
	*amqp.Delivery
	ch *Channel
}

// CreateChannel has same behavior as Channel.CreateChannel()
func (d *Delivery) CreateChannel() (*Channel, error) {
	return d.ch.CreateChannel()
}

// CreateTxChannel has same behavior as Channel.CreateTxChannel()
func (d *Delivery) CreateTxChannel() (*Channel, error) {
	return d.ch.CreateTxChannel()
}

func (d *Delivery) Ack() {
	d.Delivery.Ack(false)
}

func (d Delivery) Nack() {
	d.Delivery.Nack(false, false)
}

func (c *Channel) Qos(count int) error {
	return c.Channel.Qos(count, 0, false)
}

func (ch *Channel) Publish(exchange, key string, msg interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	m := amqp.Publishing{
		Body: data,
	}
	return ch.Channel.Publish(exchange, key, false, false, m)
}

func (ch *Channel) Consume(name string, handler interface{}) error {
	dc, err := ch.Channel.Consume(name, "", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}
	v := reflect.ValueOf(handler)
	mType := reflect.TypeOf(handler).In(0)
	go func() {
		for {
			select {
			case d, ok := <-dc:
				if !ok {
					return
				}
				msg := reflect.New(mType).Interface()
				err := json.Unmarshal(d.Body, msg)
				if err != nil {
					log.Printf("cap err: %s", err)
					continue
				}
				delivery := Delivery{
					Delivery: &d,
					ch:       ch,
				}
				in := []reflect.Value{reflect.ValueOf(msg).Elem(), reflect.ValueOf(delivery)}
				go func() { v.Call(in) }()
			}
		}
	}()
	return nil
}

type Session struct {
	Exchange string
	Name     string
	Key      string
}

func (c *Cap) AlwaysApply(s *Session) {
	c.Always(func() {
		ch, err := c.CreateChannel()
		if err != nil {
			return
		}
		defer ch.Close()

		if err := ch.ExchangeDeclare(
			s.Exchange,   // name of the exchange
			"fanout",     // type
			true,         // durable
			false,        // delete when complete
			false,        // internal
			false,        // noWait
			amqp.Table{}, // arguments
		); err != nil {
			log.Error(err)
			return
		}

		if _, err := ch.QueueDeclare(
			s.Name,       // name of the queue
			true,         // durable
			false,        // delete when usused
			false,        // exclusive
			false,        // noWait
			amqp.Table{}, // arguments

		); err != nil {
			log.Error(err)
			return
		}

		if err := ch.QueueBind(
			s.Name,       // name of the queue
			s.Key,        // bindingKey
			s.Exchange,   // sourceExchange
			false,        // noWait
			amqp.Table{}, // arguments
		); err != nil {
			log.Error(err)
		}
	})
}

// IsConnectionError checks the given error if it is a connection error or not
func IsConnectionError(err error) bool {
	amqpErr, ok := err.(*amqp.Error)
	if !ok {
		return false
	}
	switch amqpErr.Code {
	case 302, 501, 504:
		return true
	}
	return false
}
