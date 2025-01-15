package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"time"
)

// event struct to represent processing and repair events
type Event struct {
	time  int    // event timestamp
	etype string // can be done or repair
}

// A PriorityQueue implements heap.Interface and holds (*pointers to) Events
type PriorityQueue []*Event

// All PQ method implementations
func (pq PriorityQueue) Len() int           { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool { return pq[i].time < pq[j].time }
func (pq PriorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x any) {
	*pq = append(*pq, x.(*Event))
}

func (pq *PriorityQueue) Pop() interface{} {
	if len(*pq) == 0 {
		return nil
	}
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Peek() (*Event, error) {
	if len(*pq) == 0 {
		return nil, fmt.Errorf("priority queue is empty")
	}
	return (*pq)[0], nil
}

func NewPQ() *PriorityQueue {
	return &PriorityQueue{}
}

// simulation struct
type Simulation struct {
	// event queue implemented as a heap to process the most recent events
	eventQ *PriorityQueue
	// total number of jobs we need to process
	jobCount int
	// number of parallel processing units we have
	processors int
	// probability of the processing unit failing/breaking
	breakChance float32
	// amount of time it takes to get the processing unit fixed and up and running
	repairTime int
	// latency for serving requests
	processingTime int
}

// func to run the sim and return 3 duration percentages (50, 90 and 100)
func (s *Simulation) run(seed int64) (percentileTime [3]int) {
	rgen := rand.New(rand.NewSource(seed))
	eventChannel := make(chan Event)
	doneChannel := make(chan struct{})
	var inflight, broken int

	// create a go routine for each processing unit, each proccessor will have its own go routine
	for i := 0; i < s.processors; i++ {
		go func() {
			for {
				// dice roll to see if the pu breaks
				// append a repair event to the event channel
				if rgen.Float32() < s.breakChance {
					repairTime := s.repairTime + int(rgen.NormFloat64()*float64(s.repairTime)/4)
					eventChannel <- Event{etype: "REPAIR", time: repairTime}
					time.Sleep(time.Duration(repairTime) * time.Millisecond)
				}
				// otherwise, process the event and append a done event to the event channel
				processingTime := s.processingTime + int(rgen.NormFloat64()*float64(s.processingTime)/4)
				eventChannel <- Event{etype: "DONE", time: processingTime}
				time.Sleep(time.Duration(processingTime) * time.Millisecond)
			}
		}()
	}

	// central event loop, listens on eventChannel, channels in Go are FIFO so they behave like queues
	go func() {
		for done := 0; done < s.jobCount; {
			// select statements are blocking because it waits for the channel to become ready for communication before proceeding
			// this allows for concurrent AND synchronous event processing which is the behavior we want for this type of simulation
			select {
			// select will block and wait for a message to be received on the event channel
			case event := <-eventChannel:
				if event.etype == "REPAIR" {
					broken--
				} else if event.etype == "DONE" {
					done++
					inflight--
				}
			}
		}
		doneChannel <- struct{}{}
	}()

	<-doneChannel
	return
}

func main() {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	// simulate a gas station serving X cars with Y pumps
	SIM := Simulation{
		eventQ:         &pq,
		jobCount:       1000,
		processors:     12,
		breakChance:    0.05, // percentage
		repairTime:     10,   // min
		processingTime: 5,    // min
	}

	// Add initial event to start the simulation
	heap.Push(SIM.eventQ, &Event{
		time:  0,
		etype: "DONE",
	})

	res := SIM.run(100)

	// print out results
	fmt.Printf("Time to finish: 50%% = %d, 75%% = %d, 100%% = %d\n", res[0], res[1], res[2])
	fmt.Printf("Relative Time to X%%: 50%% = 1x, 75%% = %.2fx, 100%% = %.2fx\n", float32(res[1])/float32(res[0]), float32(res[2])/float32(res[0]))
}
