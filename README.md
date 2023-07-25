CSV file [with NYC taxi trips for 2018](https://data.cityofnewyork.us/Transportation/2018-Yellow-Taxi-Trip-Data/t29m-gskq) should be downloaded to data folder.

Dirty code to experiment with:
- fanin/fanout algorithms for large file processing
- small use of generics
- time series
- profiling / tracing


```sh
go run cmd/main.go --help        
  -g int
        number of goroutines used (default Runtime.NumCPU())
  -metric string
        metric to compute: metric|distance (default "counter")
  -profile
        save profiles in data folder
  -trace
        save tracer info in data folder
```

