package datachan

import (
	"container/heap"
	"reflect"
	"sort"
	"sync"
)

// Sort sorts the output of the previous stage in ascending order,
// using keyer to extract the sorting Key.
//
// Keyer must be a function like func(T)Comparable, where the output
// is a comparable type (int, uint, float, string).
//
// If the number of records is less than MaxBeforeSpill this stage
// works from memory. Otherwise it spills its contents to disk, and
// later uses merge sort to sort the whole data.
func (s *Stage) Sort(MaxBeforeSpill int, keyer T) *Stage {
	keyFun := reflect.ValueOf(keyer)
	if keyFun.Kind() != reflect.Func {
		panic("Reduce Keyer argument must be a function")
	}
	if keyFun.Type().NumIn() != 1 {
		panic("Reduce function output must support method Key(T)Comparable")
	}
	if !keyFun.Type().Out(0).Comparable() {
		panic("Reduce function output must support method Key(T)Comparable")
	}
	if keyFun.Type().NumOut() != 1 {
		panic("Reduce function output must support method Key(T)Comparable")
	}

	output := reflect.MakeChan(s.output.Type(), MaxBeforeSpill)

	readersChan := make(chan (<-chan *keyValuePair), 16)
	filesWG := sync.WaitGroup{}

	var executor func()
	executor = func() {
		// Process and accumulate all the inputs in batches
		accArr := make([]*keyValuePair, 0, MaxBeforeSpill)

		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			eKey := keyFun.Call([]reflect.Value{e})[0]

			pv := keyValuePairPool.Get().(*keyValuePair)
			pv.Key = eKey.Interface()
			pv.Value = e.Interface()

			accArr = append(accArr, pv)

			// Spill if too much records are on memory
			if len(accArr) >= MaxBeforeSpill {
				filesWG.Add(1)
				go func(partialArray []*keyValuePair) {
					sort.Sort(sortByKey(partialArray))
					tmpName := spillKeyValuePairToDisk(partialArray)
					readersChan <- readKeyValuePairFromDisk(tmpName)
					filesWG.Done()
				}(accArr)

				accArr = make([]*keyValuePair, 0, MaxBeforeSpill)
			}
		}

		// At this point all the input have been partially sorted
		// and a final merge sort is pending
		if len(accArr) > 0 {
			sort.Sort(sortByKey(accArr))
			readersChan <- readKeyValuePairFromMemory(accArr)
		}
		filesWG.Done()
	}

	var sortMerger func()
	sortMerger = func() {
		kvProviders := make([]<-chan *keyValuePair, 0)
		for kvProvider := range readersChan {
			kvProviders = append(kvProviders, kvProvider)
		}

		// Generate the priority queue and fill it
		pq := make(kvPairPriorityQueue, 0, len(readersChan))
		for _, ch := range kvProviders {
			chValue, ok := <-ch
			if ok {
				pq = append(pq, &kvMerge{
					value: chValue,
					src:   ch,
				})
			}
		}
		heap.Init(&pq)

		for pq.Len() > 0 {
			item := pq.PopAndRefill()
			output.Send(reflect.ValueOf(item.Value))
		}

		output.Close()
	}

	filesWG.Add(1)
	go executor()
	go func() {
		filesWG.Wait()
		close(readersChan)
	}()
	go sortMerger()

	return &Stage{output}
}
