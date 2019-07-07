// Package Datachan provides a Map Reduce like programming environment. It
// takes care of type castings, chaining stages, launching workers,
// spilling data to disk and other repetitive and bored tasks.
//
// Unlike other Hadoop or Spark, Datachan works locally. Its main use case
// is local data processing, without using a lot of RAM and starting
// computations as soon as possible, without orchestration overhead.
//
// Partially based on https://gist.github.com/icecrime/67399480c9a10b48fadc .
package datachan

import (
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"
)

func init() {
	gob.Register(reflect.Value{})
}

type Stage struct {
	output reflect.Value
}

func makeChannel(t reflect.Type, chanDir reflect.ChanDir, buffer int) reflect.Value {
	ctype := reflect.ChanOf(chanDir, t)
	return reflect.MakeChan(ctype, buffer)
}

func makeMap(key, elem reflect.Type) reflect.Value {
	var mapType reflect.Type
	mapType = reflect.MapOf(key, elem)
	return reflect.MakeMap(mapType)
}

type T interface{}

// Source takes a channel of any kind and transmits it to the next stage
func Source(input T) *Stage {
	value := reflect.ValueOf(input)
	if value.Kind() != reflect.Chan {
		panic("Source argument must be a channel")
	}

	etype := value.Type().Elem()
	output := makeChannel(etype, reflect.BothDir, 0)
	go func() {
		for v, ok := value.Recv(); ok; v, ok = value.Recv() {
			output.Send(v)
		}
		output.Close()
	}()

	return &Stage{output}
}

// Map takes a number of workers and a func(input T1, output chan<- T2), and applies that
// function to each element that comes from the previous stage.
// The function must emit the values through the output channel
func (s *Stage) Map(workers int, f T) *Stage {
	value := reflect.ValueOf(f)
	if value.Kind() != reflect.Func {
		panic("Map argument must be a function")
	}

	// Checks that the map function accepts two arguments
	ftype := value.Type()
	if ftype.NumIn() != 2 {
		panic("Map argument must be a function that receives two values")
	}

	// Checks constrains of output/emit channel
	otype := ftype.In(1)
	if otype.Kind() != reflect.Chan {
		panic("Second argument of Map function must be a channel")
	}
	if otype.ChanDir() != reflect.SendDir {
		panic("Second argument of Map function must be a send channel")
	}

	// Checks that the value received by Map function is the same as
	// the value generated in previous stage
	itype := ftype.In(0)
	if itype != s.output.Type().Elem() {
		panic(fmt.Sprintf("Input of Map stage function missmatch. Expected %v, found %v", s.output.Type().Elem(), itype))
	}

	output := makeChannel(otype.Elem(), reflect.BothDir, workers)
	wg := sync.WaitGroup{}
	var executor func()
	executor = func() {
		defer wg.Done()
		i := 0
		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			value.Call([]reflect.Value{e, output})
			i++
			if i > 100 {
				wg.Add(1)
				go executor()
				return
			}
		}
	}

	go func() {
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go executor()
		}
		wg.Wait()
		output.Close()
	}()

	return &Stage{output}
}

// Sink returns a <-chan interface{} that can be used to retrieve the
// computation result, one record at a time.
func (s *Stage) Sink(bufferSize int) <-chan T {
	output := make(chan T, bufferSize)
	go func() {
		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			output <- e.Interface()
		}
		close(output)
	}()
	return output
}

// Tee duplicates the output of one stage into two stages.
func (s *Stage) Tee() (*Stage, *Stage) {
	o1 := reflect.MakeChan(s.output.Type(), ChanBufferDefault)
	o2 := reflect.MakeChan(s.output.Type(), ChanBufferDefault)
	go func() {
		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			o1.Send(e)
			o2.Send(e)
		}
		o1.Close()
		o2.Close()
	}()
	return &Stage{o1}, &Stage{o2}
}

// Merge sends the output of several stages into a single one. The input stages
// must have the same output type as the previous one, that is the one that determines
// the stage output type.
func (s *Stage) Merge(stages ...*Stage) *Stage {
	output := reflect.MakeChan(s.output.Type(), ChanBufferDefault)

	go func() {
		cases := make([]reflect.SelectCase, 0, len(stages)+1)

		for _, stage := range stages {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: stage.output,
			})
		}
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: s.output,
		})

		for len(cases) > 0 {
			i, v, ok := reflect.Select(cases)
			if ok {
				output.Send(v)
			} else {
				cases = append(cases[:i], cases[i+1:]...)
			}
		}

		output.Close()
	}()
	return &Stage{output}
}

// Filter takes a number of workers and a func(input T1) bool, and applies that
// function to each element that comes from the previous stage.
// If the function returns false the value is dropped
func (s *Stage) Filter(workers int, f T) *Stage {
	value := reflect.ValueOf(f)
	if value.Kind() != reflect.Func {
		panic("Filter argument must be a function")
	}

	// Checks that the filter function accepts one arguments
	ftype := value.Type()
	if ftype.NumIn() != 1 {
		panic("Filter argument must be a function that receives one values")
	}

	// Checks that the value received by Map function is the same as
	// the value generated in previous stage
	itype := ftype.In(0)
	if itype != s.output.Type().Elem() {
		panic(fmt.Sprintf("Input of Filter stage function missmatch. Expected %v, found %v", s.output.Type().Elem(), itype))
	}

	// Checks that return value is boolean
	if ftype.NumOut() != 1 {
		panic("Filter function must return exactly one boolean value")
	}
	otype := ftype.Out(0)
	if otype.Kind() != reflect.Bool {
		panic("Filter function output must be boolean")
	}

	output := reflect.MakeChan(s.output.Type(), ChanBufferDefault)
	wg := sync.WaitGroup{}
	var executor func()
	executor = func() {
		defer wg.Done()
		i := 0
		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			preserve := value.Call([]reflect.Value{e})
			if preserve[0].Bool() {
				output.Send(e)
			}

			i++
			if i > 10000 {
				wg.Add(1)
				go executor()
				return
			}
		}
	}

	go func() {
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go executor()
		}
		wg.Wait()
		output.Close()
	}()

	return &Stage{output}
}
