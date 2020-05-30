package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)

var startTime time.Time

func Init() {
	startTime = time.Now()
}

func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

func ArraySource(a ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		buffer := make([]byte, 8)
		bytesRead := 0
		for {
			n, err := reader.Read(buffer)
			bytesRead += n
			if err != nil || (chunkSize != -1 && bytesRead > chunkSize) {
				break
			}
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
		}
		close(out)
	}()
	return out
}

func WriteSink(writer io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		writer.Write(buffer)
	}
}

func InMemSort(in <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		a := []int{}
		// Read
		for v := range in {
			a = append(a, v)
		}
		fmt.Println("Read done:", time.Now().Sub(startTime))
		// Sort
		sort.Ints(a)
		fmt.Println("In Memory Sort done:", time.Now().Sub(startTime))
		// Output
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		for {
			v1, ok1 := <-in1
			v2, ok2 := <-in2
			if ok1 || ok2 {
				if !ok2 {
					out <- v1
				} else if !ok1 {
					out <- v2
				} else if v1 <= v2 {
					out <- v1
					out <- v2
				} else {
					out <- v2
					out <- v1
				}
			} else {
				break
			}
		}
		fmt.Println("Merge Done:", time.Now().Sub(startTime))
		close(out)
	}()
	return out
}

func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	m := len(inputs) / 2
	return Merge(MergeN(inputs[:m]...), MergeN(inputs[m:]...))
}
