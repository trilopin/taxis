package stream

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

// stream makes the fanIn mecahnism for a io.Reader
// It creates a channel which is early returned and then
// reads line by line a file and push each line though
// the channel
func stream(r io.Reader) chan string {
	ch := make(chan string)
	scanner := bufio.NewScanner(r)
	scanner.Scan() //skip header
	go func() {
		defer close(ch)
		for scanner.Scan() {
			ch <- scanner.Text()
		}
	}()
	return ch
}

// ProcessFile concurrently uses the data pushed to the fanIn channel
// returned by stream through a function fn
//
// fn implementation should be like:
//
//	func(chan string) {
//		// ... preprocess code (allocation, etc..)
//
//		for line := range ch {
//			// process each line
//		}
//
//		// ... postprocess code (result sending, etc..)
//	}
func ProcessFile(path string, g int, fn func(chan string)) error {

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("can not process file %s: %v", path, err)
	}

	ch := stream(f)
	var wg sync.WaitGroup
	wg.Add(g)

	for i := 0; i < g; i++ {
		go func() {
			defer wg.Done()
			fn(ch)
		}()
	}
	wg.Wait()
	return nil
}
