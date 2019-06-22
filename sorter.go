package datachan

import (
	"container/heap"
	"os"
	"reflect"
	"sort"
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

	output := reflect.MakeChan(s.output.Type(), 0)

	var executor func()
	executor = func() {
		// Process and accumulate all the inputs in batches
		accArr := make([]*keyValuePair, 0, MaxBeforeSpill)
		tmpFiles := make([]string, 0)
		defer func(files []string) {
			for _, f := range files {
				os.Remove(f)
			}
		}(tmpFiles)
		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			eKey := keyFun.Call([]reflect.Value{e})[0]
			pv := &keyValuePair{
				Key:   eKey.Interface(),
				Value: e.Interface(),
			}
			accArr = append(accArr, pv)

			// Spill if too much records are on memory
			if len(accArr) >= MaxBeforeSpill {
				sort.Sort(sortByKey(accArr))
				tmpName := spillKeyValuePairToDisk(accArr)
				tmpFiles = append(tmpFiles, tmpName)

				accArr = make([]*keyValuePair, 0, MaxBeforeSpill)
			}
		}

		// At this point all the input have been partially sorted
		// and a final merge sort is pending
		sort.Sort(sortByKey(accArr))

		readersChan := make([]<-chan *keyValuePair, 0, len(tmpFiles)+1)
		for _, tmpFile := range tmpFiles {
			readersChan = append(readersChan, readKeyValuePairFromDisk(tmpFile))
		}
		readersChan = append(readersChan, readKeyValuePairFromMemory(accArr))

		// Generate the priority queue and fill it
		pq := make(kvPairPriorityQueue, 0, len(readersChan))
		for _, ch := range readersChan {
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

	go executor()

	return &Stage{output}
}
