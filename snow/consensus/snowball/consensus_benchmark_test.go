// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package snowball

import (
	"math"
	"math/rand"
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/prometheus/client_golang/prometheus"
)

func BenchmarkSnowball(b *testing.B) {
	tests := []struct {
		name      string
		factory   Factory
		numColors int
		params    Parameters
	}{
		{
			name:      "snowball tree-1",
			factory:   TreeFactory{},
			numColors: 1,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
		{
			name:      "snowball tree-3",
			factory:   TreeFactory{},
			numColors: 3,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
		{
			name:      "snowball tree-15",
			factory:   TreeFactory{},
			numColors: 15,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
		{
			name:      "snowball tree-127",
			factory:   TreeFactory{},
			numColors: 127,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
		{
			name:      "flat snowball-1",
			factory:   FlatFactory{},
			numColors: 1,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
		{
			name:      "flat snowball-3",
			factory:   FlatFactory{},
			numColors: 3,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
		{
			name:      "flat snowball-15",
			factory:   FlatFactory{},
			numColors: 15,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
		{
			name:      "flat snowball-127",
			factory:   FlatFactory{},
			numColors: 127,
			params: Parameters{
				Metrics: prometheus.NewRegistry(),
				K:       20, Alpha: 15, BetaVirtuous: math.MaxInt32, BetaRogue: math.MaxInt32,
			},
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			SnowballBenchmark(b, test.factory, test.numColors, test.params)
		})
	}
}

func SnowballBenchmark(b *testing.B, factory Factory, numColors int, params Parameters) {
	seed := int64(0)

	network := Network{}
	network.Initialize(params, numColors)

	rand.Seed(seed)

	consensus := factory.New()
	network.AddNode(consensus)

	pref := consensus.Preference()

	votes := ids.Bag{}
	votes.AddCount(pref, params.Alpha)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consensus.RecordPoll(votes)
	}
}
