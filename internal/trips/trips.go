package trips

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// https://data.cityofnewyork.us/Transportation/2018-Yellow-Taxi-Trip-Data/t29m-gskq
type Trip struct {
	VendorID             int
	TPEPPickupDatetime   int64
	TPEPDropoffDatetime  int64
	PassengerCount       int
	TripDistance         float64
	RatecodeID           int
	StoreAndFWDFlag      string
	PULocationID         int
	DOLocationID         int
	PaymentType          int
	FareAmount           float64
	extra                float64
	MTATax               float64
	TipAmount            float64
	TollsAmount          float64
	ImprovementSurcharge float64
	TotalAmount          float64
}

type ParseOpt func([]string, *Trip) error

func WithTPEPPickupDatetime() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.TPEPPickupDatetime, err = parseTime(parts[1])
		if err != nil {
			return err
		}
		return nil
	}
}

func WithTPEPDropoffDatetime() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.TPEPDropoffDatetime, err = parseTime(parts[2])
		if err != nil {
			return err
		}
		return nil
	}
}

func WithVendorID() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.VendorID, err = strconv.Atoi(parts[0])
		if err != nil {
			return err
		}
		return nil
	}
}

func WithRateCodeID() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.RatecodeID, err = strconv.Atoi(parts[5])
		if err != nil {
			return err
		}
		return nil
	}
}

func WithPassengers() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.PassengerCount, err = strconv.Atoi(parts[3])
		if err != nil {
			return err
		}
		return nil
	}
}
func WithTripDistance() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.TripDistance, err = strconv.ParseFloat(parts[4], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithPULocationID() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.PULocationID, err = strconv.Atoi(parts[7])
		if err != nil {
			return err
		}
		return nil
	}
}

func WithDOLocationID() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.PULocationID, err = strconv.Atoi(parts[8])
		if err != nil {
			return err
		}
		return nil
	}
}

func WithPaymentType() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.PaymentType, err = strconv.Atoi(parts[9])
		if err != nil {
			return err
		}
		return nil
	}
}

func WithFareAmount() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.FareAmount, err = strconv.ParseFloat(parts[10], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithExtra() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.extra, err = strconv.ParseFloat(parts[11], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithMTATax() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.MTATax, err = strconv.ParseFloat(parts[12], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithTipAmount() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.TipAmount, err = strconv.ParseFloat(parts[13], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithTollsAmount() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.TollsAmount, err = strconv.ParseFloat(parts[14], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithImprovementSurcharge() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.ImprovementSurcharge, err = strconv.ParseFloat(parts[15], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithTotalAmount() ParseOpt {
	return func(parts []string, t *Trip) error {
		var err error
		t.TotalAmount, err = strconv.ParseFloat(parts[16], 64)
		if err != nil {
			return err
		}
		return nil
	}
}

func fromCSVLine(line string, opts ...ParseOpt) (*Trip, error) {
	parts := strings.Split(line, ",")
	if len(parts) != 17 {
		return nil, fmt.Errorf("malformed line, wanted 17 comma-separated fields, got %d", len(parts))
	}

	trip := Trip{}
	for _, opt := range opts {
		if err := opt(parts, &trip); err != nil {
			return nil, err
		}
	}
	return &trip, nil
}

func parseTime(text string) (int64, error) {
	// Example date "04/20/2018 09:57:26 PM"
	t, err := time.Parse("01/02/2006 03:04:05 PM", text)
	if err != nil {
		return 0, err
	}
	return t.Unix(), nil
}
