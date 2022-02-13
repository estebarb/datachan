//go:build go1.18

package datachan

import (
	"hash/fnv"
	"sync"
)

type Stage[T any] struct {
	output chan T // Stage input channel
}

func SourceChan[TIn any](values chan TIn) *Stage[TIn] {
	return &Stage[TIn]{
		output: values,
	}
}

func SourceSlice[TIn any](values []TIn) *Stage[TIn] {
	out := &Stage[TIn]{
		output: make(chan TIn),
	}

	go func() {
		defer close(out.output)
		for _, v := range values {
			out.output <- v
		}
	}()

	return out
}

func SinkChan[T any](stage *Stage[T]) chan T {
	return stage.output
}

func SinkSlice[T any](stage *Stage[T]) []T {
	out := make([]T, 0)
	for v := range stage.output {
		out = append(out, v)
	}
	return out
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

type Mapper[TIn any, TOut any] func(input TIn, output chan<- TOut)
type SimpleMapper[TIn any, TOut any] func(input TIn) TOut

func MapSimple[TIn any, TOut any](s *Stage[TIn], workers int, f SimpleMapper[TIn, TOut]) *Stage[TOut] {
	outputStage := &Stage[TOut]{
		output: make(chan TOut),
	}

	go func() {
		defer close(outputStage.output)
		launchWorkers(workers,
			func() {
				for value := range s.output {
					outputStage.output <- f(value)
				}
			})
	}()

	return outputStage
}

func Map[TIn any, TOut any](s *Stage[TIn], workers int, f Mapper[TIn, TOut]) *Stage[TOut] {
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

type Hasher[T any] func(T) int

func Shuffle[T any](stage *Stage[T], workers int, buckets int, hasher Hasher[T]) []*Stage[T] {
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

func pipeChannel[T any](wg *sync.WaitGroup, src chan T, dst chan T) {
	defer wg.Done()
	for v := range src {
		dst <- v
	}
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

func IntHasher(i int) int {
	h := fnv.New32()
	for i > 0 {
		h.Write([]byte{byte(i & 0xff), byte((i >> 8) & 0xff), byte((i >> 16) & 0xff), byte((i >> 24) & 0xff)})
		i >>= 32
	}
	return int(h.Sum32())
}
