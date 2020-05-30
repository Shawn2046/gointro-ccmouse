package main

import (
	"bufio"
	"fmt"
	"imooc.com/ccmouse/gointro/pipeline"
	"os"
	"strconv"
)

func main() {
	p := createNetworkPipeline("large.in", 819200000, 4)
	writeToFile("large.out", p)
	printFile("large.out")
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReaderSource(file, 64)
	for v := range p {
		fmt.Println(v)
	}
}

func writeToFile(filename string, in <-chan int) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	pipeline.WriteSink(writer, in)
}

func createPipeline(filename string, fileSize, chunkCount int) <-chan int {
	chunkSize := fileSize / chunkCount
	pipeline.Init()

	sortResult := []<-chan int{}
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)
		sortResult = append(sortResult, pipeline.InMemSort(source))

	}

	return pipeline.MergeN(sortResult...)
}

func createNetworkPipeline(filename string, fileSize, chunkCount int) <-chan int {
	chunkSize := fileSize / chunkCount
	pipeline.Init()

	sortAddr := []string{}
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)

		addr := ":" + strconv.Itoa(7000+i)
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}
	sortResults := []<-chan int{}
	for _, v := range sortAddr {
		net := pipeline.NetworkSource(v)
		sortResults = append(sortResults, net)
	}
	return pipeline.MergeN(sortResults...)
}
