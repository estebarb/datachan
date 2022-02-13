package datachan

import (
	"reflect"
	"sync"
)

func (s *Stage) Fold(MaxBeforeSpill int, keyer, f T) *Stage {
	fv := reflect.ValueOf(f)
	if fv.Kind() != reflect.Func {
		panic("Combiner argument must be a function")
	}

	tf := fv.Type()

	if tf.NumOut() != 1 {
		panic("The Combiner function must return one fv")
	}

	if tf.NumIn() != 3 {
		panic("The Combiner function must receive three values")
	}

	if tf.In(1) != tf.Out(0) {
		panic("The Combiner function output and accumulator types must be equal")
	}

	keyFun := reflect.ValueOf(keyer)
	if keyFun.Kind() != reflect.Func {
		panic("Combiner Keyer argument must be a function")
	}
	if keyFun.Type().NumIn() != 1 {
		panic("Combiner function output must support method Key(T)Comparable")
	}
	if !keyFun.Type().Out(0).Comparable() {
		panic("Combiner function output must support method Key(T)Comparable")
	}
	if keyFun.Type().NumOut() != 1 {
		panic("Combiner function output must support method Key(T)Comparable")
	}

	output := reflect.MakeChan(s.output.Type(), MaxBeforeSpill)
	wg := sync.WaitGroup{}

	var executor func()
	executor = func() {
		// Process and accumulate all the inputs in batches
		accMap := makeMap(keyFun.Type().Out(0), tf.Out(0))
		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			eKey := keyFun.Call([]reflect.Value{e})
			val := accMap.MapIndex(eKey[0])
			if val.IsValid() {
				newVal := fv.Call([]reflect.Value{eKey[0], val, e})
				accMap.SetMapIndex(eKey[0], newVal[0])
			} else {
				// Spill BEFORE inserting if too much records are on memory
				// This allows to implement Reduce as:
				// Combiner(X,...).Sort(...).Fold(1,...)
				if accMap.Len() >= 1 {
					iter := accMap.MapRange()
					for iter.Next() {
						output.Send(iter.Value())
						accMap.SetMapIndex(iter.Key(), reflect.Value{})
					}
				}
				accMap.SetMapIndex(eKey[0], e)
			}
		}

		// At this point all the input have been partially reduced
		// and a final reduction is pending
		iter := accMap.MapRange()
		for iter.Next() {
			output.Send(iter.Value())
			accMap.SetMapIndex(iter.Key(), reflect.Value{})
		}

		wg.Wait()
		output.Close()
	}

	go executor()

	return &Stage{output}
}

// Combiner takes a number of records to reduce before spilling to disk,
// and a function that reduces those records. The reduce function must be
// func (key T0, acc T1, value T1) T1.
//
// The combiner sends the partially reduced records to the next stage,
// usually a Sort and another Combiner stages. For example, Reduce
// is implemented using Combiner->Sort->Combiner.
func (s *Stage) Combiner(MaxBeforeSpill int, keyer, f T) *Stage {
	fv := reflect.ValueOf(f)
	if fv.Kind() != reflect.Func {
		panic("Combiner argument must be a function")
	}

	tf := fv.Type()

	if tf.NumOut() != 1 {
		panic("The Combiner function must return one fv")
	}

	if tf.NumIn() != 3 {
		panic("The Combiner function must receive three values")
	}

	if tf.In(1) != tf.Out(0) {
		panic("The Combiner function output and accumulator types must be equal")
	}

	keyFun := reflect.ValueOf(keyer)
	if keyFun.Kind() != reflect.Func {
		panic("Combiner Keyer argument must be a function")
	}
	if keyFun.Type().NumIn() != 1 {
		panic("Combiner function output must support method Key(T)Comparable")
	}
	if !keyFun.Type().Out(0).Comparable() {
		panic("Combiner function output must support method Key(T)Comparable")
	}
	if keyFun.Type().NumOut() != 1 {
		panic("Combiner function output must support method Key(T)Comparable")
	}

	output := reflect.MakeChan(s.output.Type(), MaxBeforeSpill)
	wg := sync.WaitGroup{}

	var executor func()
	executor = func() {
		// Process and accumulate all the inputs in batches
		accMap := makeMap(keyFun.Type().Out(0), tf.Out(0))
		for e, ok := s.output.Recv(); ok; e, ok = s.output.Recv() {
			eKey := keyFun.Call([]reflect.Value{e})
			val := accMap.MapIndex(eKey[0])
			if val.IsValid() {
				newVal := fv.Call([]reflect.Value{eKey[0], val, e})
				accMap.SetMapIndex(eKey[0], newVal[0])
			} else {
				// Spill BEFORE inserting if too much records are on memory
				// This allows to implement Reduce as:
				// Combiner(X,...).Sort(...).Combiner(1,...)
				if accMap.Len()+1 >= MaxBeforeSpill {
					wg.Add(1)
					go spillCombinerToChannel(accMap, output, &wg)
					accMap = makeMap(keyFun.Type().Out(0), tf.Out(0))
				}
				accMap.SetMapIndex(eKey[0], e)
			}
		}

		// At this point all the input have been partially reduced
		// and a final reduction is pending
		wg.Add(1)
		go spillCombinerToChannel(accMap, output, &wg)
		accMap = makeMap(keyFun.Type().Out(0), tf.Out(0))

		wg.Wait()
		output.Close()
	}

	go executor()

	return &Stage{output}
}

func spillCombinerToChannel(accMap reflect.Value, outputChan reflect.Value, wg *sync.WaitGroup) {
	iter := accMap.MapRange()
	for iter.Next() {
		outputChan.Send(iter.Value())
	}
	if wg != nil {
		wg.Done()
	}
}
