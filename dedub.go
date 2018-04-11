package main

import (
	"bufio"
	"flag"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	N "github.com/OneOfOne/xxhash"
)

var wg sync.WaitGroup

var d = 0
var u = 0

// Create the threadsafe map.
var sm sync.Map

//Job struct for memory efficiency
type Job struct {
	ID        int
	Duplicate bool
	Work      string
}

func producer(jobs chan<- *Job, inpath string) {
	defer wg.Done()
	g, _ := os.Open(inpath)
	scanner := bufio.NewScanner(g)
	var q = 0
	for scanner.Scan() {
		r := scanner.Text()
		jobs <- &Job{Work: r, ID: q}
		q++ //incrementing id
	}
	defer g.Close()
	close(jobs)
}

func writer(results <-chan *Job, done chan<- bool, path string, savedub bool) {
	var linebreak string
	if runtime.GOOS == "windows" {
		linebreak = "\r\n"
	} else {
		linebreak = "\n"
	}
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	defer f.Close()
	g, _ := os.OpenFile(path[:len(path)-4]+"_duplicates.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	defer g.Close()
	for {
		j, more := <-results
		if more {
			if j.Duplicate == false {
				_, _ = f.WriteString(j.Work + linebreak)
				u++
				continue
			}
			if j.Duplicate == true {
				if savedub {
					_, _ = g.WriteString(j.Work + linebreak)
				}
				d++
			}
		} else {
			done <- true
		}
	}
}

func worker(jobs <-chan *Job, results chan<- *Job) {
	defer wg.Done()
	for {
		j, more := <-jobs
		if more {
			h32 := N.New32()
			h32.WriteString(strings.TrimSpace(j.Work))
			y := h32.Sum32()
			v, d := sm.Load(y)
			if v == true {
				j.Duplicate = true
				results <- j
				continue
			}
			if d == false {
				j.Duplicate = false
				sm.Store(y, true)
				results <- j
				continue
			}

		} else {
			return
		}
	}
}

func Round(x float64) float64 {
	t := math.Trunc(x)
	if math.Abs(x-t) >= 0.5 {
		return t + math.Copysign(1, x)
	}
	return t
}

func main() {
	threads := flag.Int("t", runtime.NumCPU(), "Number of Goroutines")
	inpath := flag.String("i", "1.txt", "Path of input.txt")
	outpath := flag.String("o", "1out.txt", "Path of out.txt")
	savedub := flag.Bool("d", true, "Write duplicates to disk")
	flag.Parse()

	var jobs = make(chan *Job)
	var results = make(chan *Job)
	var done = make(chan bool, 1)

	for w := 1; w <= *threads; w++ {
		wg.Add(1)
		go worker(jobs, results)
	}
	wg.Add(1)
	go producer(jobs, *inpath)
	go writer(results, done, *outpath, *savedub)
	start := time.Now()
	wg.Wait()
	close(results)
	<-done
	elapsed := time.Since(start)
	log.Println("Unique lines:", u, "Duplicate lines:", d, "Total lines:", u+d)
	log.Printf("Done. Time elapsed: %s", elapsed)
}
