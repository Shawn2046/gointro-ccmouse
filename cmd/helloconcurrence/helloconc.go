package main

import "fmt"

func main() {
	ch := make(chan string)
	for i := 0; i < 500; i++ {
		go printHello(i, ch)
	}
	for {
		msg := <-ch
		fmt.Println(msg)
	}
	close(ch)
}

func printHello(i int, ch chan string) {
	ch <- fmt.Sprintf("Hello World from %d\n", i)
}
