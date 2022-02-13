//go:build go1.18

package datachan

import (
	"sort"
	"testing"
)

func generateInts(n int) chan int {
	ch := make(chan int)
	go func() {
		for i := 0; i < n; i++ {
			ch <- i
		}
		close(ch)
	}()
	return ch
}

func TestSourceChan(t *testing.T) {
	stage := SourceChan(generateInts(10))
	ch := SinkChan(stage)
	for i := 0; i < 10; i++ {
		if v := <-ch; v != i {
			t.Errorf("Expected %d, got %d", i, v)
		}
	}
}

func TestSourceSlice(t *testing.T) {
	sl := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	stage := SourceSlice(sl)
	ch := SinkChan(stage)
	for i := 0; i < 10; i++ {
		if v := <-ch; v != i {
			t.Errorf("Expected %d, got %d", i, v)
		}
	}
}

func TestSinkSlice(t *testing.T) {
	stage := SourceChan(generateInts(10))
	sl := SinkSlice(stage)
	if len(sl) != 10 {
		t.Errorf("Expected 10, got %d", len(sl))
	}
	for i := 0; i < 10; i++ {
		if sl[i] != i {
			t.Errorf("Expected %d, got %d", i, sl[i])
		}
	}
}

func TestMapSimple(t *testing.T) {
	stage := SourceChan(generateInts(100))
	stage = MapSimple(stage, 1, func(input int) int {
		return input * 2
	})
	ch := SinkChan(stage)
	for i := 0; i < 10; i++ {
		if v := <-ch; v != i*2 {
			t.Errorf("Expected %d, got %d", i*2, v)
		}
	}
}

func TestMapSimpleParallel(t *testing.T) {
	stage := SourceChan(generateInts(100))
	stage = MapSimple(stage, 10, func(input int) int {
		return input * 2
	})
	sl := SinkSlice(stage)
	sort.Ints(sl)

	if len(sl) != 100 {
		t.Errorf("Expected 100, got %d", len(sl))
	}

	for i := 0; i < 100; i++ {
		if sl[i] != i*2 {
			t.Errorf("Expected %d, got %d", i*2, sl[i])
		}
	}
}

func TestMap(t *testing.T) {
	stage := SourceChan(generateInts(100))
	stage = Map(stage, 10, func(in int, out chan<- int) {
		for i := 0; i < in; i++ {
			out <- in
		}
	})
	sl := SinkSlice(stage)
	sort.Ints(sl)

	cp := 0
	for i := 0; i < 100; i++ {
		for j := 0; j < i; j++ {
			if v := sl[cp]; v != i {
				t.Errorf("Expected %d, got %d", i, v)
			}
			cp++
		}
	}
}
