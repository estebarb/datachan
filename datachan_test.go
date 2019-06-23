package datachan

import (
	"encoding/gob"
	"sync"
	"testing"
)

func init() {
	gob.Register(&kvii{})
}

func generateInts(limit int) chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < limit; i++ {
			out <- i
		}
		close(out)
	}()
	return out
}

func TestSource(t *testing.T) {
	in := generateInts(5)
	src := Source(in)
	sink := src.Sink(1)
	for i := 0; i < 5; i++ {
		v := <-sink
		if v.(int) != i {
			t.Fatal("Expected", i, "got", v)
		}
	}
}

func TestMap(t *testing.T) {
	in := generateInts(5)
	src := Source(in)
	mapper := src.Map(2, func(v int, output chan<- int) {
		output <- v + v
	})
	sink := mapper.Sink(1)
	acc := 0
	for i := 0; i < 5; i++ {
		acc += (<-sink).(int)
	}
	if acc != 20 {
		t.Fatal("Expected", 20, "got", acc)
	}
}

type kvii struct {
	Key   int
	Value int
}

func TestReduce(t *testing.T) {
	in := generateInts(5)
	flow := Source(in).Map(2, func(v int, output chan<- *kvii) {
		output <- &kvii{Key: v % 2, Value: v}
	}).Reduce(50,
		func(n *kvii) int {
			return n.Key
		},
		func(key int, acc *kvii, value *kvii) *kvii {
			acc.Value += value.Value
			return acc
		}).Sink(1)

	acc0 := 0
	acc1 := 0
	for i := 0; i < 2; i++ {
		item := (<-flow).(*kvii)
		if item.Key%2 == 0 {
			acc0 += item.Value
		} else {
			acc1 += item.Value
		}
	}
	if acc0 != 6 {
		t.Fatal("Expected", 6, "got", acc0)
	}
	if acc1 != 4 {
		t.Fatal("Expected", 4, "got", acc1)
	}

	_, ok := <-flow
	if ok {
		t.Fatal("expected closed channel")
	}
}

func TestReduceBigger(t *testing.T) {
	in := generateInts(256000)
	flow := Source(in).Map(8, func(v int, output chan<- *kvii) {
		output <- &kvii{Key: v % 2, Value: v}
	}).Reduce(16,
		func(n *kvii) int {
			return n.Key
		},
		func(key int, acc *kvii, value *kvii) *kvii {
			acc.Value += value.Value
			return acc
		}).Sink(1)

	acc0 := 0
	acc1 := 0
	for i := 0; i < 2; i++ {
		item := (<-flow).(*kvii)
		if item.Key%2 == 0 {
			acc0 += item.Value
		} else {
			acc1 += item.Value
		}
	}

	var expected0, expected1 int
	for i := 0; i < 256000; i++ {
		if i%2 == 0 {
			expected0 += i
		} else {
			expected1 += i
		}
	}

	if acc0 != expected0 {
		t.Fatal("Expected", expected0, "got", acc0)
	}
	if acc1 != expected1 {
		t.Fatal("Expected", expected1, "got", acc1)
	}

	_, ok := <-flow
	if ok {
		t.Fatal("expected closed channel")
	}
}

func TestCombiner(t *testing.T) {
	limit := 10
	spillSize := 3

	flow := Source(generateInts(limit)).Map(4, func(v int, output chan<- int) {
		output <- v
	}).Combiner(spillSize,
		func(n int) int {
			return n
		},
		func(key int, acc int, value int) int {
			return acc + value
		}).Sink(1)

	obtained := make([]int, limit)
	for i := 0; i < limit; i++ {
		item := <-flow
		obtained[i] = item.(int)
	}

	_, ok := <-flow
	if ok {
		t.Fatal("expected closed channel")
	}
}

func TestCombinerSorter(t *testing.T) {
	limit := 10

	keyer := func(n int) int {
		return n
	}

	flow := Source(generateInts(limit)).Map(4, func(v int, output chan<- int) {
		output <- v
	}).Combiner(5,
		keyer,
		func(key int, acc int, value int) int {
			return acc + value
		}).Sort(4, keyer).Sink(1)

	obtained := make([]int, limit)
	for i := 0; i < limit; i++ {
		item := <-flow
		obtained[item.(int)] = item.(int)
	}

	for i := 0; i < limit; i++ {
		if i != obtained[i] {
			t.Fatal("Expected item", i, "to be", i, "found", obtained[i])
		}
	}

	_, ok := <-flow
	if ok {
		t.Fatal("expected closed channel")
	}
}

func TestReduceWithBasicSpilling(t *testing.T) {
	limit := 10
	spillSize := 3

	flow := Source(generateInts(limit)).Map(8, func(v int, output chan<- int) {
		output <- v
	}).Reduce(spillSize,
		func(n int) int {
			return n
		},
		func(key int, acc int, value int) int {
			return acc + value
		}).Sink(1)

	obtained := make([]int, limit)
	for i := 0; i < limit; i++ {
		item := (<-flow).(int)
		obtained[i] = item
	}

	expected := make([]int, limit)
	for i := 0; i < limit; i++ {
		expected[i%limit] += i
	}

	for k, v := range obtained {
		if expected[k] != v {
			t.Fatal("Expected", expected[k], "for key", k, "found", obtained[k], "instead")
		}
	}

	_, ok := <-flow
	if ok {
		t.Fatal("expected closed channel")
	}
}

func TestReduceWithSpilling(t *testing.T) {
	limit := 512000
	spillSize := 4000
	groups := 5000

	flow := Source(generateInts(limit)).Map(8, func(v int, output chan<- *kvii) {
		output <- &kvii{Key: v % groups, Value: v}
	}).Reduce(spillSize,
		func(n *kvii) int {
			return n.Key
		},
		func(key int, acc *kvii, value *kvii) *kvii {
			acc.Value += value.Value
			return acc
		}).Sink(1)

	obtained := make([]int, groups)
	for i := 0; i < groups; i++ {
		item := (<-flow).(*kvii)
		//if int(item.Key.(int)) != i {
		//	t.Fatal("Key", i, "expected to be", i, "found", item.Key.(int), "instead")
		//}
		obtained[i] = item.Value
	}

	expected := make([]int, groups)
	for i := 0; i < limit; i++ {
		expected[i%groups] += i
	}

	for k, v := range expected {
		if obtained[k] != v {
			t.Fatal("Expected", v, "for key", k, "found", obtained[k], "instead")
		}
	}

	_, ok := <-flow
	if ok {
		t.Fatal("expected closed channel")
	}
}

func TestTee(t *testing.T) {
	total := 1000
	f1, f2 := Source(generateInts(total)).Tee()
	wg := sync.WaitGroup{}
	checker := func(id string, s *Stage) {
		sink := s.Sink(1)
		i := 0
		for value := range sink {
			if i != value {
				t.Fatal(id, "expected", i, "(first time) received", value)
			}
			i++
		}

		if i < total {
			t.Fatal("Expected more values, got just", i)
		}
		wg.Done()
	}
	wg.Add(2)
	go checker("first", f1)
	go checker("second", f2)
	wg.Wait()
}

func TestTeeMerge(t *testing.T) {
	limit := 100
	f1, f2 := Source(generateInts(100)).Tee()
	final := f1.Merge(f2)
	var totalSum int

	for iv := range final.Sink(1) {
		totalSum += iv.(int)
	}

	if totalSum != 2*(limit*(limit-1)/2) {
		t.Fatal("Expected sum ", 2*(limit*(limit-1)/2), "got", totalSum)
	}
}

func TestStage_Filter(t *testing.T) {
	limit := 10000
	flow := Source(generateInts(limit)).Filter(10, func(value int) bool {
		return value%2 == 0
	}).Sink(1)

	acc := 0
	for value := range flow {
		if value.(int)%2 == 1 {
			t.Fatal("Odd values should have been filtered away, found", value)
		} else {
			acc += value.(int)
		}
	}

	expected := 0
	for i := 0; i < limit; i++ {
		if i%2 == 0 {
			expected += i
		}
	}

	if acc != expected {
		t.Fatal("Sum expected to be", expected, "got", acc)
	}
}
