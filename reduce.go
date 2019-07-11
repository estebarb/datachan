package datachan

import (
	"sync"
)

const (
	ChanBufferDefault = 4096
)

type keyValuePair struct {
	Key   interface{}
	Value interface{}
}

var keyValuePairPool = sync.Pool{
	New: func() interface{} {
		return new(keyValuePair)
	},
}

var TopkeyValuePriorityQueueNodePool = sync.Pool{
	New: func() interface{} {
		return new(TopkeyValuePriorityQueueNode)
	},
}

// Reduce takes a number of records to accumulate before spilling to disk,
// and a function that reduces those records. The reduce function must be
// func (key T0, acc T1, value T1) T1
func (s *Stage) Reduce(MaxBeforeSpill int, keyer, f T) *Stage {
	return s.Combiner(MaxBeforeSpill, keyer, f).Sort(MaxBeforeSpill, keyer).Fold(1, keyer, f)
}
