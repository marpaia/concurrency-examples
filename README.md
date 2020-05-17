# Concurrency Examples

This repository is a grab bag of examples which illustrate different concurrency primatives in various languages that I like to use for various data processing tasks.

## Worker Pool in Python

The file [`workers.py`](./workers.py) shows creating a multiprocessing worker pool and using it to process each line in several CSVs independently. In this example, I create and serialize [`tf.train.Example`](https://www.tensorflow.org/tutorials/load_data/tfrecord) records from each line but this is just a naive example.

## Synchronized File Writes in Go

The file [`synchronized_workers.go`](./synchronized_workers.go) show creating a pool of goroutines, processing each line in several CSVs on the pool, and then collecting the final results into one file. This builds on the "Worker Pool in Python" example by introducing coordination. If this degree of coordination is required, I think Go offers nicer primatives than Python. Unfortunately, the domain doesn't always allow for this trade-off to be made in practice