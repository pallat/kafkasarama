package main

import (
	"fmt"
)

func main() {
	ch := make(chan struct{})
	signal := make(chan struct{})

	go func() {
		signal <- struct{}{}
	}()

	select {
	case <-ch:
		fmt.Println("got ch")
	case <-signal:
		fmt.Println("got signal")
	}

	fmt.Println("end")
}

func mainOfLazyPrint() {
	ch := make(chan struct{})
	chString := make(chan string)

	go delayPrint(ch, chString)

	chString <- "1"

	go func() {
		chString <- "2"
	}()
	go func() {
		chString <- "3"
	}()

	<-ch
	<-ch
	<-ch

}

func delayPrint(ch chan<- struct{}, chString <-chan string) {
	for {
		fmt.Println(<-chString)
		ch <- struct{}{}
	}
}
