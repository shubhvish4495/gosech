package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"

	"github.com/shubhvish4495/gosech"
)

const jobIdBaseName = "jobID"

func main() {
	serv, err := gosech.NewService("localhost:61613", nil, "test.task.queue")
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < 10; i++ {
		jobName := fmt.Sprintf("%s%d", jobIdBaseName, i)
		log.Println("jobID ", jobName)
		err := serv.SendMessage(jobName, []byte(fmt.Sprintf("%d", i)))
		if err != nil {
			log.Println(err)
			return
		}
	}

	log.Println("message sent successfully, now moving forward to ingestion")

	serv.RegisterFuncWithJobID("jobID1", func(m []byte) error {
		fmt.Println(string(m))
		if rand.Int()%2 == 0 {
			return errors.New("new random error passed")
		}
		return nil
	})

	serv.RegisterFuncWithJobID("jobID2", func(m []byte) error {
		fmt.Println("from jobID2")
		fmt.Println(string(m))
		return nil
	})

	if err := serv.StartMultiFuncProcessing(); err != nil {
		log.Println(err)
		return
	}
}
