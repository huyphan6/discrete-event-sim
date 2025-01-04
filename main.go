package main

import (
	"container/heap"
	"fmt"
	"math/rand"
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

	time := 0
	done := 0
	inflight := 0
	broken := 0

	for done < s.jobCount {
		nextEvent := heap.Pop(s.eventQ).(*Event)
		time = nextEvent.time

		switch nextEvent.etype {
		case "REPAIR":
			broken--
		case "DONE":
			done++
			inflight--
		}

		// schedule new repairs if processors are broken
		for broken < s.processors && rgen.Float32() < s.breakChance {
			broken++
			// probabilistic repair yime
			repairTime := s.repairTime + int(rgen.NormFloat64()*float64(s.repairTime)/4)
			if repairTime < 0 {
				repairTime = 0
			}
			heap.Push(s.eventQ, &Event{
				time:  time + repairTime,
				etype: "REPAIR",
			})
		}

		// schedule new processing tasks for healthy processors
		for inflight < s.processors-broken && done+inflight < s.jobCount {
			inflight++
			processingTime := s.processingTime + int(rgen.NormFloat64()*float64(s.processingTime)/4)
			if processingTime < 0 {
				processingTime = 0
			}
			heap.Push(s.eventQ, &Event{
				time:  time + processingTime,
				etype: "DONE",
			})
		}

		// calculate percentiles
		percentDone := 100 * done / s.jobCount
		percentiles := []int{50, 75, 100}
		for idx, p := range percentiles {
			if percentDone >= p && percentileTime[idx] == 0 {
				percentileTime[idx] = time
			}
		}
	}

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
