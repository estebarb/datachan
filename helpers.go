package datachan

import (
	"log"
	"reflect"
)

// FromStringSlice returns a stage seeded with strings
// in the array.
func FromStringSlice(input []string) *Stage {
	output := make(chan string)

	go func() {
		for _, v := range input {
			output <- v
		}
		close(output)
	}()

	return Source(output)
}

// Pipe sends the output of the previous stage to a channel,
// and optionally closes the channel after the transmision is done.
//
// This is useful for nesting datachan flows, for example, within
// a Map operation.
func (s *Stage) Pipe(output T, close bool) {
	value := reflect.ValueOf(output)
	if value.Kind() != reflect.Chan {
		panic("Pipe argument must be a channel")
	}

	for v, ok := s.output.Recv(); ok; v, ok = s.output.Recv() {
		value.Send(v)
	}

	if close {
		value.Close()
	}
}

// LogCompletion logs when the previous stage finished. Useful for logging or debugging
func (s *Stage) LogCompletion(name string, bufferSize int) *Stage {
	output := reflect.MakeChan(s.output.Type(), bufferSize)

	go func() {
		for v, ok := s.output.Recv(); ok; v, ok = s.output.Recv() {
			output.Send(v)
		}
		log.Println("Finished stage", name)
		output.Close()
	}()

	return &Stage{output}
}

// Count returns the count of elements entering from previous stage. It is best used with Tee.
func (s *Stage) Count() <-chan uint {
	output := make(chan uint)

	go func() {
		count := uint(0)
		for _, ok := s.output.Recv(); ok; _, ok = s.output.Recv() {
			count++
		}
		output <- count
		close(output)
	}()

	return output
}

// Drop drops all the elements from previous stage. It returns the same type as input.
func (s *Stage) Drop(bufferSize int) *Stage {
	output := reflect.MakeChan(s.output.Type(), bufferSize)

	go func() {
		for _, ok := s.output.Recv(); ok; _, ok = s.output.Recv() {
		}
		output.Close()
	}()

	return &Stage{output}
}

func (s *Stage) Limit(limit int) *Stage {
	if limit < 0 {
		return s
	}

	output := reflect.MakeChan(s.output.Type(), limit)

	go func() {
		i := 0
		for value, ok := s.output.Recv(); ok; value, ok = s.output.Recv() {
			if i < limit {
				i++
				output.Send(value)
			} else if i == limit {
				output.Close()
				i++
			}
		}
		if i < limit {
			output.Close()
		}
	}()

	return &Stage{output}
}
