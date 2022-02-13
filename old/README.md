Datachan: A Map Reduce like Framework
=====================================

[![](https://godoc.org/github.com/estebarb/datachan?status.svg)](http://godoc.org/github.com/estebarb/datachan)
[![Build Status](https://travis-ci.org/estebarb/datachan.svg?branch=master)](https://travis-ci.org/estebarb/datachan)


Datachan is a library that allows Map Reduce paradigm like programming,
chaining stages and taking care of launching workers, spilling to disk
and other repetitive and bored tasks.

Datachan supports the following methods:
- Source:   Transmit data from a channel
- Filter:   Filters output of the previous stage
- Map:      Applies a function to each element in the previous stage
- Reduce:   Performs a reduction over previous stage data. Internally it
            handles spilling data to disk, so it can process data bigger than
            memory without issues.
- Sort:     Sorts previous stage output, using merge sort. Spills data to disk
            if required.
- Combiner: Partial reduce of previous stage data. It streams to the next stage
            when the number of records reach a threshold.
- Tee:      Duplicates output of one stage into two new stages.
- Merge:    Merges several stages into a single one.
- Sink:     Transmit output to a channel