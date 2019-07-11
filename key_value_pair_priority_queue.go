package datachan

import (
	"container/heap"
)

// A StringKeyedMerge is something we manage in a priority queue.
type kvMerge struct {
	value *keyValuePair
	src   <-chan *keyValuePair
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A StringKeyedPriorityQueue implements heap.Interface and holds Items.
type kvPairPriorityQueue []*kvMerge

func (pq kvPairPriorityQueue) Len() int { return len(pq) }

func (pq kvPairPriorityQueue) Less(i, j int) bool {
	a := pq[i].value
	b := pq[j].value
	return a.less(b)
}

func (pq kvPairPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *kvPairPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*kvMerge)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *kvPairPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *kvPairPriorityQueue) PopAndRefill() *keyValuePair {
	node := heap.Pop(pq).(*kvMerge)

	refillItem, ok := <-node.src
	if ok {
		heap.Push(pq, &kvMerge{
			value: refillItem,
			src:   node.src,
		})
	}

	return node.value
}

// A TopkeyValuePriorityQueueNode is something we manage in a priority queue.
type TopkeyValuePriorityQueueNode struct {
	value *keyValuePair
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A TopKeyValuePriorityQueue implements heap.Interface and holds Items.
type TopKeyValuePriorityQueue []*TopkeyValuePriorityQueueNode

func (pq TopKeyValuePriorityQueue) Len() int { return len(pq) }

func (pq TopKeyValuePriorityQueue) Less(i, j int) bool {
	a := pq[i].value
	b := pq[j].value
	return a.less(b)
}

func (pq TopKeyValuePriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *TopKeyValuePriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TopkeyValuePriorityQueueNode)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *TopKeyValuePriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
