package cap

//todo: cd style declare/bind

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

var ReconnectDelay = time.Second * 5

var (
	DifferentConnErr      = errors.New("connection has been changed")
	ConnNotInitializedErr = errors.New("connection not initialized yet")
)

var log = logrus.New()

type Cap struct {
	addr string

	connectReq chan bool
	getConnReq chan getConnReq
	funcReq    chan func()
}

type Connection struct {
	conn *amqp.Connection
	cap  *Cap
}

func newConnection(c *amqp.Connection, cap *Cap) *Connection {
	return &Connection{
		conn: c,
		cap:  cap,
	}
}

func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	return &Channel{
		Channel: ch,
		conn:    c,
	}, nil
}

type getConnReq struct {
	wait  bool
	reply chan *Connection
}

func Open(addr string) (*Cap, error) {
	_, err := amqp.ParseURI(addr)
	if err != nil {
		return nil, err
	}
	cap := &Cap{
		addr:       addr,
		connectReq: make(chan bool, 0),
		getConnReq: make(chan getConnReq, 0),
	}
	go cap.connectLoop()
	go cap.connect()
	return cap, nil
}

func (c *Cap) connect() {
	go func() { c.connectReq <- true }()
}

func (c *Cap) connectLoop() {
	var (
		connecting  bool
		conn        *Connection
		connection  = make(chan *amqp.Connection, 0)
		getConnReqs = make([]chan *Connection, 0)
	)

	for {
		select {
		case <-c.connectReq:
			if connecting {
				continue
			}
			connecting = true

			go func() {
				cn, err := amqp.Dial(c.addr)
				if err != nil {
					log.Infof("couldn't connect to server err: %s", err)
					time.Sleep(ReconnectDelay)
					connection <- nil
					return
				}

				go func() {
					err := <-cn.NotifyClose(make(chan *amqp.Error, 0))
					log.Infof("connection lost err: %s", err)
					connection <- nil
				}()

				connection <- cn
				log.Info("connected")
			}()

		case cn := <-connection:
			connecting = false

			if cn == nil {
				c.connect()
			} else {
				conn = newConnection(cn, c)
				for _, reply := range getConnReqs {
					reply <- conn
				}
			}

		case getConnReq := <-c.getConnReq:
			if connecting && getConnReq.wait {
				getConnReqs = append(getConnReqs, getConnReq.reply)
			} else {
				getConnReq.reply <- conn
			}
		}
	}
}

func (c *Cap) AlwaysChannel(f func(ch *Channel) error) {
	for {
		ch, err := c.Channel(true)
		if err != nil {
			log.Println(err)
			continue
		}
		go func() {
			<-ch.NotifyClose(make(chan *amqp.Error, 1))
			c.AlwaysChannel(f)
		}()
		if err := f(ch); err != nil {
			break
		}
	}
}

func (c *Cap) getConnection(wait bool) *Connection {
	req := getConnReq{
		wait:  wait,
		reply: make(chan *Connection),
	}
	c.getConnReq <- req
	return <-req.reply
}

type Channel struct {
	*amqp.Channel
	conn *Connection
}

func (c *Cap) Channel(wait bool) (*Channel, error) {
	conn := c.getConnection(wait)

	if conn == nil {
		return nil, ConnNotInitializedErr
	}

	return conn.Channel()
}

func (c *Cap) TxChannel(wait bool) (*Channel, error) {
	ch, err := c.Channel(wait)
	if err != nil {
		return nil, err
	}
	return ch, ch.Tx()
}

func (ch *Channel) MakeChannel() (*Channel, error) {
	conn := ch.conn.cap.getConnection(false)

	if conn == nil {
		return nil, ConnNotInitializedErr
	}

	if conn.conn.LocalAddr() == ch.conn.conn.LocalAddr() {
		return conn.Channel()
	}

	return nil, DifferentConnErr
}

func (ch *Channel) MakeTxChannel() (*Channel, error) {
	chn, err := ch.MakeChannel()
	if err != nil {
		return nil, err
	}
	return chn, chn.Tx()
}

type Delivery struct {
	*amqp.Delivery
	ch *Channel
}

func (d *Delivery) Channel() (*Channel, error) {
	return d.ch.MakeChannel()
}

func (d *Delivery) MakeTxChannel() (*Channel, error) {
	return d.ch.MakeTxChannel()
}

func (d *Delivery) Ack() {
	d.Delivery.Ack(false)
}

func (d Delivery) Nack(requeue bool) {
	d.Delivery.Nack(false, requeue)
}

func (c *Channel) Qos(count int) error {
	return c.Channel.Qos(count, 0, false)
}

//todo NotifyReturn
func (ch *Channel) Publish(exchange, key string, msg interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	m := amqp.Publishing{
		Body:         data,
		DeliveryMode: 2,
	}
	return ch.Channel.Publish(exchange, key, true, false, m)
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

func (c *Cap) Apply(s *Session) {
	for {
		ch, err := c.Channel(true)
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
			if IsConnectionError(err) {
				continue
			}
			log.Fatalln(err)
		}

		if _, err := ch.QueueDeclare(
			s.Name,       // name of the queue
			true,         // durable
			false,        // delete when usused
			false,        // exclusive
			false,        // noWait
			amqp.Table{}, // arguments

		); err != nil {
			if IsConnectionError(err) {
				continue
			}
			log.Fatalln(err)
		}

		if err := ch.QueueBind(
			s.Name,       // name of the queue
			s.Key,        // bindingKey
			s.Exchange,   // sourceExchange
			false,        // noWait
			amqp.Table{}, // arguments
		); err != nil {
			if IsConnectionError(err) {
				continue
			}
			log.Fatalln(err)
		}

		break
	}
}

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
