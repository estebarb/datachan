package datachan

import (
	"bufio"
	"container/heap"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
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

	readersChan := make(chan string, 16)
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
				go executor()
				break
			}
		}

		sort.Sort(sortByKey(accArr))
		tmpName := spillKeyValuePairToDisk(accArr)
		readersChan <- tmpName

		filesWG.Done()
	}

	var sortMerger func()
	sortMerger = func() {
		kvProvidersFiles := make([]string, 0)
		for kvProvider := range readersChan {
			kvProvidersFiles = append(kvProvidersFiles, kvProvider)
		}

		kvProviders := make([]<-chan *keyValuePair, 0)
		for _, kvProvider := range kvProvidersFiles {
			kvProviders = append(kvProviders, readKeyValuePairFromDisk(kvProvider))
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

func (p *keyValuePair) keyEquals(q *keyValuePair) bool {
	a := p.Key
	b := q.Key
	switch a.(type) {
	case string:
		return a.(string) == b.(string)
	case int:
		return a.(int) == b.(int)
	case uint:
		return a.(uint) == b.(uint)
	case float64:
		return a.(float64) == b.(float64)
	case float32:
		return a.(float32) == b.(float32)
	default:
		panic(fmt.Sprint("Type is not sortable: only String, Int, Uint and Float are allowed, given: ",
			a, " and ", b,
			" (", reflect.TypeOf(a), ")"))
	}
}

func (p *keyValuePair) less(q *keyValuePair) bool {
	a := p.Key
	b := q.Key
	switch a.(type) {
	case string:
		return a.(string) < b.(string)
	case int:
		return a.(int) < b.(int)
	case uint:
		return a.(uint) < b.(uint)
	case float64:
		return a.(float64) < b.(float64)
	case float32:
		return a.(float32) < b.(float32)
	default:
		panic(fmt.Sprint("Type is not sortable: only String, Int, Uint and Float are allowed, given: ",
			a, " and ", b,
			" (", reflect.TypeOf(a), ")"))
	}
}

type sortByKey []*keyValuePair

func (s sortByKey) Len() int      { return len(s) }
func (s sortByKey) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortByKey) Less(i, j int) bool {
	return s[i].less(s[j])
}

func spillKeyValuePairToDisk(arr []*keyValuePair) string {
	tmpfile, err := ioutil.TempFile("", "datachan_partial")
	if err != nil {
		log.Fatal(err)
	}
	defer tmpfile.Close()

	bufTmp := bufio.NewWriter(tmpfile)
	defer bufTmp.Flush()

	enc := gob.NewEncoder(bufTmp)
	for k, v := range arr {
		err := enc.Encode(v)
		keyValuePairPool.Put(v)
		arr[k] = nil
		if err != nil {
			panic(fmt.Sprint("Unable to encode", v, ":", err))
		}
	}

	return tmpfile.Name()
}

func readKeyValuePairFromDisk(file string) <-chan *keyValuePair {
	// The buffer value was selected testing with a Word Count program
	output := make(chan *keyValuePair, 1)

	go func() {
		defer os.Remove(file)
		fd, err := os.Open(file)
		if err != nil {
			log.Fatalln(err)
		}
		defer fd.Close()
		buf := bufio.NewReader(fd)
		dec := gob.NewDecoder(buf)

		more := true
		for more {
			data := keyValuePairPool.Get().(*keyValuePair)
			err := dec.Decode(data)
			if err != nil && err != io.EOF {
				log.Fatal(err)
			} else if err == io.EOF {
				more = false
				close(output)
			} else {
				output <- data
			}
		}
	}()

	return output
}

func readKeyValuePairFromMemory(arr []*keyValuePair) <-chan *keyValuePair {
	output := make(chan *keyValuePair)

	go func() {
		for k, v := range arr {
			output <- v
			arr[k] = nil
		}
		close(output)
	}()

	return output
}
