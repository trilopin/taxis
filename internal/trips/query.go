package trips

import (
	"log"
	"runtime"
	"sort"
	"time"

	"github.com/trilopin/taxis/internal/platform/stream"
)

type DataItem[V int64 | float64] struct {
	T   int64
	Val V
}

type QueryOpts struct {
	// G is the number of goroutines used to parse the file
	// runtime.NumCPU will be used if values is <= 0
	G int

	// File path to csv the data source
	Path string

	// Don't process results older than FromDate. This doesn't
	// prevent the algorithm from reading the entire file since
	// it's not necessarily sorted nor processed in order.
	// Limit will be ignored if value is nil (default)
	FromDate *time.Time

	// Don't process results newer than ToDate. This doesn't
	// prevent the algorithm from reading the entire file since
	// it's not necessarily sorted nor processed in order.
	// Limit will be ignored if value is nil (default)
	ToDate *time.Time

	// Interval is the number of seconds between each group of
	// resulting data
	Interval int64
}

func Counter(opts QueryOpts) ([]DataItem[int64], error) {
	if opts.G <= 0 {
		opts.G = runtime.NumCPU()
	}

	data := make([]DataItem[int64], 0)
	globalAcc := make(map[int64]int64, 0)
	res := make(chan map[int64]int64, opts.G)

	stream.ProcessFile(opts.Path, opts.G, func(ch chan string) {
		acc := make(map[int64]int64, 0)
		for line := range ch {
			trip, err := fromCSVLine(line, WithTPEPPickupDatetime())
			if err != nil {
				log.Println(err)
				continue
			}
			// exclude if it's older than passed filter opts.fromDate
			if opts.FromDate != nil && time.Unix(trip.TPEPPickupDatetime, 0).Before(*opts.FromDate) {
				continue
			}

			// exclude if it's more recent than passed filter opts.toDate
			if opts.ToDate != nil && time.Unix(trip.TPEPPickupDatetime, 0).After(*opts.ToDate) {
				continue
			}

			// pickup=24,interval=5 -> t=20 and series [0,5,10,15,20]
			t := trip.TPEPPickupDatetime - (trip.TPEPPickupDatetime % opts.Interval)
			acc[t]++
		}
		res <- acc
	})

	// collect results
	for i := 0; i < opts.G; i++ {
		d := <-res
		for t, count := range d {
			globalAcc[t] += count
		}
	}

	// convert to array and sort
	for t, count := range globalAcc {
		data = append(data, DataItem[int64]{t, count})
	}

	sort.Slice(data, func(i, j int) bool {
		return data[i].T < data[j].T
	})

	return data, nil
}

func AvgDistance(opts QueryOpts) ([]DataItem[float64], error) {
	type AccItem struct {
		count int64
		sum   float64
	}

	if opts.G <= 0 {
		opts.G = runtime.NumCPU()
	}

	data := make([]DataItem[float64], 0)
	globalAcc := make(map[int64]AccItem, 0)
	res := make(chan map[int64]AccItem, opts.G)

	stream.ProcessFile(opts.Path, opts.G, func(ch chan string) {
		acc := make(map[int64]AccItem, 0)
		for line := range ch {
			trip, err := fromCSVLine(
				line,
				WithTPEPPickupDatetime(),
				WithTripDistance(),
			)
			if err != nil {
				log.Println(err)
				continue
			}
			// exclude if it's older than passed filter opts.fromDate
			if opts.FromDate != nil && time.Unix(trip.TPEPPickupDatetime, 0).Before(*opts.FromDate) {
				continue
			}

			// exclude if it's more recent than passed filter opts.toDate
			if opts.ToDate != nil && time.Unix(trip.TPEPPickupDatetime, 0).After(*opts.ToDate) {
				continue
			}

			// pickup=24,interval=5 -> t=20 and series [0,5,10,15,20]
			t := trip.TPEPPickupDatetime - (trip.TPEPPickupDatetime % opts.Interval)
			v := acc[t]
			v.count++
			v.sum += trip.TripDistance
			acc[t] = v
		}
		res <- acc
	})

	// collect results
	for i := 0; i < opts.G; i++ {
		d := <-res
		for t, partial := range d {
			v := globalAcc[t]
			v.count += partial.count
			v.sum += partial.sum
			globalAcc[t] = v

		}
	}

	// convert to array and sort
	for t, d := range globalAcc {
		data = append(data, DataItem[float64]{t, d.sum / float64(d.count)})
	}

	sort.Slice(data, func(i, j int) bool {
		return data[i].T < data[j].T
	})

	return data, nil
}
