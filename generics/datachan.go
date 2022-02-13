//go:build go1.18

package main

import (
	"hash/fnv"
	"sync"
	"fmt"
)

type Stage[T any] struct {
	output chan T
}

func SourceChan[TIn any](values chan TIn) *Stage[TIn] {
	return &Stage[TIn]{
		output: values,
	}
}

func launchWorkers(workers int, f func()) {
	wg := &sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}
	wg.Wait()
}

func Map[TIn any, TOut any](s *Stage[TIn], workers int, f func(input TIn, output chan<- TOut)) *Stage[TOut] {
	outputChan := make(chan TOut)
	outputStage := &Stage[TOut]{
		output: outputChan,
	}
	go func() {
		defer close(outputChan)
		launchWorkers(
			workers,
			func() {
				for value := range s.output {
					f(value, outputChan)
				}
			})
	}()
	return outputStage
}

func Shuffle[T any](stage *Stage[T], workers int, buckets int, hasher func(T) int) []*Stage[T] {
	output := make([]*Stage[T], buckets)
	for i := 0; i < buckets; i++ {
		output[i] = &Stage[T]{
			output: make(chan T),
		}
	}

	go func() {
		defer func() {
			for _, s := range output {
				close(s.output)
			}
		}()
		launchWorkers(workers,
			func() {
				for value := range stage.output {
					hash := hasher(value)
					output[hash%len(output)].output <- value
				}
			})
	}()

	return output
}

func Combiner[T any, TKey comparable](stage *Stage[T], keyer func(v T) TKey, combiner func(key TKey, acc, value T) T) *Stage[T] {
	outchan := make(chan T)
	stageOut := &Stage[T]{
		output: outchan,
	}

	go func() {
		defer close(outchan)
		accdic := make(map[TKey]T)

		for value := range stage.output {
			key := keyer(value)
			acc, ok := accdic[key]
			if ok {
				accdic[key] = combiner(key, acc, value)
			} else {
				accdic[key] = value
			}
		}

		for _, v := range accdic {
			outchan <- v
		}
	}()

	return stageOut
}

func Reduce[T any, TKey comparable](stage *Stage[T], workers int, keyer func(v T) TKey, hasher func(TKey) int, combiner func(key TKey, acc, value T) T, reducer func(key TKey, acc, value T) T) *Stage[T] {
	outchan := make(chan T)
	stageOut := &Stage[T]{
		output: outchan,
	}

	if combiner != nil {
		stage = Combiner(stage, keyer, combiner)
	}
	stages := Shuffle(stage, workers, workers, func(v T) int {
		return hasher(keyer(v))
	})
	for i, s := range stages {
		stages[i] = Combiner(s, keyer, reducer)
	}

	go func() {
		defer close(outchan)
		wg := &sync.WaitGroup{}
		for _, s := range stages {
			wg.Add(1)
			go pipeChannel(wg, s.output, outchan)
		}
		wg.Wait()
	}()

	return stageOut
}

func StringHasher(str string) int {
	h := fnv.New32()
	h.Write([]byte(str))
	return int(h.Sum32())
}

func pipeChannel[T any](wg *sync.WaitGroup, src chan T, dst chan T) {
	defer wg.Done()
	for v := range src {
		dst <- v
	}
}

//func internalHashCombiner[TIn any, TKey any, TOut any](in chan TIn, workers int, keyer func(v TIn)TKey, combiner func(key TKey, acc TOut, value TIn)) {
//}

func main() {
	valuesCh := make(chan int)

	go func() {
		for i := 0; i < 15; i++ {
			valuesCh <- i % 5
		}
		close(valuesCh)
	}()

	sinit := SourceChan(valuesCh)

	s := Map(sinit, 4, func(value int, out chan<- int) {
		out <- value
	})

	s = Reduce(s, 4, func(n int) int {
		return n
	},
		func(n int) int { return int(n) },
		nil,
		func(key int, acc, value int) int {
			return acc + value
		})

	for v := range s.output {
		fmt.Println(v)
	}
	//shuffledStages := Shuffle(s, 4, 6, func(n int) int {
	//		return n
	//	})

	/*
		wg := &sync.WaitGroup{}
		for i, st := range shuffledStages {
			wg.Add(1)
			go func(bucket int, s *Stage[int]) {
				defer wg.Done()
				for v := range s.output {
					fmt.Println("Bucket", bucket, "value", v)
				}
			}(i, st)
		}
		wg.Wait()
	*/
}
