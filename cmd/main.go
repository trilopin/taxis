package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/trilopin/taxis/internal/trips"
)

var (
	profile = flag.Bool("profile", false, "save profiles in data folder")
	dotrace = flag.Bool("trace", false, "save tracer info in data folder")
	g       = flag.Int("g", runtime.NumCPU(), "number of goroutines used")
	metric  = flag.String("metric", "counter", "metric to compute: metric|distance")
)

func main() {
	flag.Parse()

	var fcpu, fmem io.WriteCloser
	if *profile {
		fcpu, _ = os.Create("data/cpu.pprof")
		defer fcpu.Close()
		if err := pprof.StartCPUProfile(fcpu); err != nil {
			log.Fatalf("can not start cpu profile: %v", err)
		}
	}

	if *dotrace {
		ftrace, err := os.Create("data/trace.out")
		if err != nil {
			log.Fatalf("failed to create trace output file: %v", err)
		}
		defer func() {
			if err := ftrace.Close(); err != nil {
				log.Fatalf("failed to close trace file: %v", err)
			}
		}()
		if err := trace.Start(ftrace); err != nil {
			log.Fatalf("failed to start trace: %v", err)
		}
		defer trace.Stop()
	}

	from, _ := time.Parse("2006-01-02", "2017-12-24")
	to, _ := time.Parse("2006-01-02", "2018-01-09")
	opts := trips.QueryOpts{
		Path:     "data/2018_Yellow_Taxi_Trip_Data.csv",
		G:        *g,
		Interval: 86400,
		FromDate: &from,
		ToDate:   &to,
	}

	switch *metric {
	case "counter":
		data, err := trips.Counter(opts)
		if err != nil {
			log.Fatal(err)
		}
		for _, d := range data {
			fmt.Println(d.T, time.Unix(d.T, 0).Format(time.RFC1123), d.Val)
		}
	case "distance":
		data, err := trips.AvgDistance(opts)
		if err != nil {
			log.Fatal(err)
		}
		for _, d := range data {
			fmt.Println(d.T, time.Unix(d.T, 0).Format(time.RFC1123), d.Val)
		}
	default:
		log.Fatalf("Unknown metric %s, wanted counter|distance", *metric)
	}

	if *profile {
		pprof.StopCPUProfile()
		runtime.GC()
		fmem, _ = os.Create("data/mem.pprof")
		defer fmem.Close()
		if err := pprof.WriteHeapProfile(fmem); err != nil {
			log.Fatalf("can not write mem profile: %v", err)
		}
	}

}
