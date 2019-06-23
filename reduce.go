package datachan

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
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

// Reduce takes a number of records to accumulate before spilling to disk,
// and a function that reduces those records. The reduce function must be
// func (key T0, acc T1, value T1) T1
func (s *Stage) Reduce(MaxBeforeSpill int, keyer, f T) *Stage {
	return s.Combiner(MaxBeforeSpill, keyer, f).Sort(MaxBeforeSpill, keyer).Fold(1, keyer, f)
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
	output := make(chan *keyValuePair, ChanBufferDefault)

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
		for _, v := range arr {
			output <- v
		}
		close(output)
	}()

	return output
}
