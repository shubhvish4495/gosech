package gosech

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/go-stomp/stomp/v3"
)

type Service struct {
	conn               *stomp.Conn
	actionFunc         func([]byte) error
	subscription       *stomp.Subscription
	jobIdActionFuncMap map[string]func([]byte) error
	dataQueue          string
}

type multiFuncMsg struct {
	Body  []byte
	JobID string
}

// NewService returns a new service object for gosech. This method expects
// the network address of the stomp server, the tlsConfguration if any and
// the name of the queue from which we want the data to be processed
func NewService(addr string, tlsCfg *tls.Config, dataQueue string) (*Service, error) {
	var netConn io.ReadWriteCloser
	// if tls config is passed we will create a tls connection
	// else we create normal connection
	if tlsCfg != nil {
		conn, err := tls.Dial("tcp", addr, tlsCfg)
		if err != nil {
			return nil, err
		}
		netConn = conn
	} else {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		netConn = conn
	}

	// create a new connection
	stompConn, err := stomp.Connect(netConn, stomp.ConnOpt.HeartBeat(time.Minute*5, time.Minute*5))
	if err != nil {
		return nil, err
	}

	// subscribe to the topic
	sub, err := stompConn.Subscribe(dataQueue, stomp.AckClientIndividual)
	if err != nil {
		return nil, err
	}

	return &Service{
		conn:               stompConn,
		subscription:       sub,
		jobIdActionFuncMap: make(map[string]func([]byte) error),
		dataQueue:          dataQueue,
	}, nil
}

// RegisterFunc registers the function to be executed
func (s *Service) RegisterFunc(f func([]byte) error) {
	s.actionFunc = f
}

// RegisterFuncWithJobID registers a function with the provided job Id
// this jobId should be unique and will be invoked when the passed message has
// this jobId mentioned in it.
func (s *Service) RegisterFuncWithJobID(jobId string, f func([]byte) error) error {
	if _, ok := s.jobIdActionFuncMap[jobId]; ok {
		return errors.New("job with the given id already exists")
	}

	s.jobIdActionFuncMap[jobId] = f
	return nil
}

// This will disconnect with the running server gracefully
func (s *Service) CloseServer() error {
	return s.conn.Disconnect()
}

// StartMultiFuncProcessing starts processing function from the job queue
// and will try to find the function for the job id passed into the message
// if we are not able to find job with the job id the message will be discarded.
// Call to this function will be blocking
func (s *Service) StartMultiFuncProcessing() error {
	log.Println("Starting with message processing for multi func")
	for {
		tx, err := s.conn.BeginWithError()
		if err != nil {
			return err
		}

		var msgData multiFuncMsg

		msg := <-s.subscription.C
		json.Unmarshal(msg.Body, &msgData)

		if f, ok := s.jobIdActionFuncMap[msgData.JobID]; !ok {
			log.Println("No job found for this message, discarding the message")

		} else {
			err = f(msgData.Body)
			if err != nil {
				log.Println("error encountered while message processing. resending it to the back of the queue")
				tx.Send(s.dataQueue, "", msg.Body)
			}
		}

		// while acking or committing the message if we encounter error we would reconnect to the sub
		// so that this message is released for anyother server to pick it up
		if err := tx.Ack(msg); err != nil {
			s.reconnectSub()
			continue
		}

		if err := tx.Commit(); err != nil {
			s.reconnectSub()
			continue
		}
	}
}

// StartProcessing will start ingesting messages on default topic.
// call to this function will be blocking
func (s *Service) StartProcessing() error {
	log.Println("Starting with message processing")
	for {
		tx, err := s.conn.BeginWithError()
		if err != nil {
			return err
		}

		msg := <-s.subscription.C
		err = s.actionFunc(msg.Body)
		if err != nil {
			log.Println("error encountered while message processing. resending it to the back of the queue")
			tx.Send(s.dataQueue, "", msg.Body)
		}

		// while acking or committing the message if we encounter error we would reconnect to the sub
		// so that this message is released for anyother server to pick it up
		if err := tx.Ack(msg); err != nil {
			s.reconnectSub()
			continue
		}

		if err := tx.Commit(); err != nil {
			s.reconnectSub()
			continue
		}

	}
}

// reconnectSub drops the connection to the sub and then reconnects to it
// this is to ensure that in case we are not getting any error while acking or
// commiting the message, stomp doesnot resend data to anyother server involved
// unless we remove the subscriber holding it. So to remove this we will reconnect
// the server
func (s *Service) reconnectSub() {
	s.subscription.Unsubscribe()
	sub, err := s.conn.Subscribe(s.dataQueue, stomp.AckClientIndividual)
	// we panic here as we need to close the connection as we are not able to reconnect to the subscription
	if err != nil {
		panic(fmt.Sprintf("error while reconnecting to sub %s", err))
	}

	s.subscription = sub

}

func (s *Service) SendMessage(jobId string, data []byte) error {
	msg := multiFuncMsg{
		Body:  data,
		JobID: jobId,
	}

	byteData, _ := json.Marshal(msg)
	if err := s.conn.Send(s.dataQueue, "", byteData); err != nil {
		return err
	}

	return nil
}
