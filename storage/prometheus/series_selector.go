// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"sync/atomic"

	"github.com/thanos-io/promql-engine/warnings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type SeriesSelector interface {
	GetSeries(ctx context.Context, shard, numShards int) ([]SignedSeries, error)
	Matchers() []*labels.Matcher
}

type SignedSeries struct {
	storage.Series
	Signature uint64
}

type seriesSelector struct {
	storage  storage.Querier
	matchers []*labels.Matcher
	hints    storage.SelectHints

	// triggerOnce coordinates series loading without sync.Once so that all
	// waiting goroutines are woken simultaneously via a channel close rather
	// than serially through sync.Mutex. This avoids the O(N) Mutex.Unlock
	// cascade when multiple callers race to load the same selector (e.g.,
	// multiple filteredSelectors sharing one seriesSelector via the pool).
	//
	// State machine: 0 = untriggered, 1 = loading, 2 = done.
	triggerOnce atomic.Uint32
	done        chan struct{} // closed (broadcast) when loading is complete
	loadErr     error
	series      []SignedSeries
}

func newSeriesSelector(storage storage.Querier, matchers []*labels.Matcher, hints storage.SelectHints) *seriesSelector {
	return &seriesSelector{
		storage:  storage,
		matchers: matchers,
		hints:    hints,
		done:     make(chan struct{}),
	}
}

func (o *seriesSelector) Matchers() []*labels.Matcher {
	return o.matchers
}

func (o *seriesSelector) GetSeries(ctx context.Context, shard int, numShards int) ([]SignedSeries, error) {
	if o.triggerOnce.Load() < 2 {
		if o.triggerOnce.CompareAndSwap(0, 1) {
			// This goroutine won the race: do the actual load.
			o.loadErr = o.loadSeries(ctx)
			close(o.done) // broadcast to all waiting goroutines at once
			o.triggerOnce.Store(2)
		} else {
			// Another goroutine is loading; wait for the broadcast.
			select {
			case <-o.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if o.loadErr != nil {
		return nil, o.loadErr
	}
	return seriesShard(o.series, shard, numShards), nil
}

func (o *seriesSelector) loadSeries(ctx context.Context) error {
	seriesSet := o.storage.Select(ctx, false, &o.hints, o.matchers...)
	i := 0
	for seriesSet.Next() {
		s := seriesSet.At()
		o.series = append(o.series, SignedSeries{
			Series:    s,
			Signature: uint64(i),
		})
		i++
	}

	for _, w := range seriesSet.Warnings() {
		warnings.AddToContext(w, ctx)
	}
	return seriesSet.Err()
}

func seriesShard(series []SignedSeries, index int, numShards int) []SignedSeries {
	start := index * len(series) / numShards
	end := min((index+1)*len(series)/numShards, len(series))

	slice := series[start:end]
	shard := make([]SignedSeries, len(slice))
	copy(shard, slice)

	for i := range shard {
		shard[i].Signature = uint64(i)
	}
	return shard
}
