// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package block

import (
	"fmt"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	latencyMetrics "github.com/ava-labs/avalanchego/utils/metrics"
	"github.com/ava-labs/avalanchego/utils/timer"
	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	buildBlock,
	parseBlock,
	getBlock,
	setPreference,
	lastAccepted prometheus.Histogram
}

func (m *metrics) Initialize(
	namespace string,
	registerer prometheus.Registerer,
) error {
	m.buildBlock = latencyMetrics.NewNanosecnodsLatencyMetric(namespace, "build_block")
	m.parseBlock = latencyMetrics.NewNanosecnodsLatencyMetric(namespace, "parse_block")
	m.getBlock = latencyMetrics.NewNanosecnodsLatencyMetric(namespace, "get_block")
	m.setPreference = latencyMetrics.NewNanosecnodsLatencyMetric(namespace, "set_preference")
	m.lastAccepted = latencyMetrics.NewNanosecnodsLatencyMetric(namespace, "last_accepted")

	errs := wrappers.Errs{}
	errs.Add(
		registerer.Register(m.buildBlock),
		registerer.Register(m.parseBlock),
		registerer.Register(m.getBlock),
		registerer.Register(m.setPreference),
		registerer.Register(m.lastAccepted),
	)
	return errs.Err
}

type MeterVM struct {
	ChainVM
	metrics
	clock timer.Clock
}

func (vm *MeterVM) Initialize(
	ctx *snow.Context,
	db database.Database,
	genesisBytes []byte,
	toEngine chan<- common.Message,
	fxs []*common.Fx,
) error {
	if err := vm.metrics.Initialize(fmt.Sprintf("metervm_%s", ctx.Namespace), ctx.Metrics); err != nil {
		return err
	}

	return vm.ChainVM.Initialize(ctx, db, genesisBytes, toEngine, fxs)
}

// BuildBlock ...
func (vm *MeterVM) BuildBlock() (snowman.Block, error) {
	start := vm.clock.Time()
	blk, err := vm.BuildBlock()
	end := vm.clock.Time()
	vm.metrics.buildBlock.Observe(float64(end.Sub(start)))
	return blk, err
}

// ParseBlock ...
func (vm *MeterVM) ParseBlock(b []byte) (snowman.Block, error) {
	start := vm.clock.Time()
	blk, err := vm.ParseBlock(b)
	end := vm.clock.Time()
	vm.metrics.parseBlock.Observe(float64(end.Sub(start)))
	return blk, err
}

// GetBlock ...
func (vm *MeterVM) GetBlock(id ids.ID) (snowman.Block, error) {
	start := vm.clock.Time()
	blk, err := vm.GetBlock(id)
	end := vm.clock.Time()
	vm.metrics.getBlock.Observe(float64(end.Sub(start)))
	return blk, err
}

// SetPreference ...
func (vm *MeterVM) SetPreference(id ids.ID) {
	start := vm.clock.Time()
	vm.SetPreference(id)
	end := vm.clock.Time()
	vm.metrics.setPreference.Observe(float64(end.Sub(start)))
}

// LastAccepted ...
func (vm *MeterVM) LastAccepted() ids.ID {
	start := vm.clock.Time()
	lastAcceptedID := vm.LastAccepted()
	end := vm.clock.Time()
	vm.metrics.lastAccepted.Observe(float64(end.Sub(start)))
	return lastAcceptedID
}
