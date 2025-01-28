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

// func to run the sim and return 3 duration percentages (50, 95, and 99)
func (s *Simulation) run(seed int64) (percentileTime [3]int) {
	rgen := rand.New(rand.NewSource(seed))
	eventChannel := make(chan Event)
	doneChannel := make(chan struct{})
	var inflight, broken int

	// create a go routine for each processing unit, each processor will have its own go routine
	for i := 0; i < s.processors; i++ {
		go func() {
			currentTime := 0 // Track cumulative time for this processor
			for {
				// dice roll to see if the pu breaks
				if rgen.Float32() < s.breakChance {
					repairTime := s.repairTime + int(rgen.NormFloat64()*float64(s.repairTime)/4)
					eventChannel <- Event{etype: "REPAIR", time: currentTime + repairTime}
					currentTime += repairTime
					time.Sleep(time.Duration(repairTime) * time.Millisecond)
				} else {
					// otherwise, process the event and append a done event to the event channel
					processingTime := s.processingTime + int(rgen.NormFloat64()*float64(s.processingTime)/4)
					eventChannel <- Event{etype: "DONE", time: currentTime + processingTime}
					currentTime += processingTime
					time.Sleep(time.Duration(processingTime) * time.Millisecond)
				}
			}
		}()
	}

	// central event loop, listens on eventChannel
	go func() {
		for done := 0; done < s.jobCount; {
			// receive a message from the event channel
			event := <-eventChannel
			if event.etype == "REPAIR" {
				broken--
			} else if event.etype == "DONE" {
				done++
				inflight--

				// Calculate the percentage of jobs done
				percentDone := 100 * done / s.jobCount
				percentiles := []int{50, 95, 99}
				for idx, p := range percentiles {
					if percentDone >= p && percentileTime[idx] == 0 {
						percentileTime[idx] = event.time // Use event.time as the cumulative time
					}
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
	fmt.Printf("Time to finish: 50%% = %d, 95%% = %d, 99%% = %d\n", res[0], res[1], res[2])
	fmt.Printf("Relative Time to X%%: 50%% = 1x, 95%% = %.2fx, 99%% = %.2fx\n", float32(res[1])/float32(res[0]), float32(res[2])/float32(res[0]))
}
