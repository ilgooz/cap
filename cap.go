package cap

import (
	"errors"
	"net"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Reconnection deplay time
var ReconnectDelay = time.Second * 5

var log = logrus.New()

var (
	differentConnErr = errors.New("connection has been changed")
)

type Cap struct {
	addr string

	conn     *amqp.Connection
	connReq  chan bool
	waitConn chan chan bool
}

// Open opens a connection to amqp server
// and re-connects if the connection drops
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

				c.conn = cn
			}()

		case is := <-connected:
			connect = !is
			if connect {
				c.connect()
			} else {
				for _, reply := range waitings {
					reply <- true
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

func (c *Cap) getConnReady() {
	wait := make(chan bool, 0)
	c.waitConn <- wait
	<-wait
}

func (c *Cap) Always(do func()) {
	c.getConnReady()
	//add: listen for die and loop
}

func (c *Cap) RegisterSession() {

}

type Channel struct {
	*amqp.Channel
	connAddr net.Addr
}

func (c *Cap) AlwaysChannel(f func(*Channel)) {

}

// Channel creates a channel for current connection
// It waits before creating a channel if there is no valid connection to server
func (c *Cap) Channel() (*Channel, error) {
	c.getConnReady()
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	return &Channel{
		Channel:  ch,
		connAddr: c.conn.LocalAddr(),
	}, nil
}

// Same as the Channel() but gives a Tx channel
func (c *Cap) TxChannel() (*Channel, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	return ch, ch.Tx()
}

//add: cd style declare/bind
