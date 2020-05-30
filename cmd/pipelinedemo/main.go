package main

import (
	"bufio"
	"fmt"
	"imooc.com/ccmouse/gointro/pipeline"
	"os"
)

func main() {
	const FILENAME = "large.in"
	const N = 102400000
	file, err := os.Create(FILENAME)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.RandomSource(N)
	writer := bufio.NewWriter(file)
	pipeline.WriteSink(writer, p)
	writer.Flush()

	file, err = os.Open(FILENAME)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p = pipeline.ReaderSource(bufio.NewReader(file), -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count > 100 {
			break
		}
	}
}

func mergeDemo() {
	p1 := pipeline.InMemSort(pipeline.ArraySource(3, 2, 6, 7, 4))
	p2 := pipeline.InMemSort(pipeline.ArraySource(1, 9, 5, 8, 4))
	p := pipeline.Merge(p1, p2)
	for v := range p {
		fmt.Println(v)
	}
}
