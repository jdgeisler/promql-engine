// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"sync/atomic"

	"github.com/prometheus/prometheus/model/labels"
)

type filteredSelector struct {
	selector *seriesSelector
	filter   Filter

	// triggerOnce coordinates series loading without sync.Once so that all
	// waiting goroutines are woken simultaneously via a channel close rather
	// than serially through sync.Mutex. This avoids the O(N) Mutex.Unlock
	// cascade when DecodingConcurrency goroutines all race to load the same
	// selector.
	//
	// State machine: 0 = untriggered, 1 = loading, 2 = done.
	triggerOnce atomic.Uint32
	done        chan struct{} // closed (broadcast) when loading is complete
	loadErr     error
	series      []SignedSeries
}

func NewFilteredSelector(selector *seriesSelector, filter Filter) SeriesSelector {
	return &filteredSelector{
		selector: selector,
		filter:   filter,
		done:     make(chan struct{}),
	}
}

func (f *filteredSelector) Matchers() []*labels.Matcher {
	return append(f.selector.matchers, f.filter.Matchers()...)
}

func (f *filteredSelector) GetSeries(ctx context.Context, shard, numShards int) ([]SignedSeries, error) {
	if f.triggerOnce.Load() < 2 {
		if f.triggerOnce.CompareAndSwap(0, 1) {
			// This goroutine won the race: do the actual load.
			f.loadErr = f.loadSeries(ctx)
			close(f.done) // broadcast to all waiting goroutines at once
			f.triggerOnce.Store(2)
		} else {
			// Another goroutine is loading; wait for the broadcast.
			select {
			case <-f.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return seriesShard(f.series, shard, numShards), nil
}

func (f *filteredSelector) loadSeries(ctx context.Context) error {
	series, err := f.selector.GetSeries(ctx, 0, 1)
	if err != nil {
		return err
	}

	var i uint64
	f.series = make([]SignedSeries, 0, len(series))
	for _, s := range series {
		if f.filter.Matches(s) {
			f.series = append(f.series, SignedSeries{
				Series:    s.Series,
				Signature: i,
			})
			i++
		}
	}

	return nil
}
