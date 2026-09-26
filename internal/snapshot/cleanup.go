// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/storage"
)

// Cleaner runs chunk cleanup independently of snapshot creation, on its
// own goroutine.
type Cleaner struct {
	logger        *slog.Logger
	config        *config.Config
	storageClient storage.ObjectStorage
	metrics       *Metrics

	wake chan struct{} // buffered(1): non-blocking "there's new work" signal

	mu              sync.Mutex
	pendingRevision int64 // highest enqueued revision not yet cleaned
	lastCleaned     int64 // highest revision fully cleaned so far

	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	started atomic.Bool
}

// NewCleaner creates a new chunk cleanup worker.
func NewCleaner(logger *slog.Logger, cfg *config.Config, storageClient storage.ObjectStorage, snapshotMetrics *Metrics) *Cleaner {
	ctx, cancel := context.WithCancel(context.Background())
	return &Cleaner{
		logger:        logger,
		config:        cfg,
		started:       atomic.Bool{},
		storageClient: storageClient,
		metrics:       snapshotMetrics,
		wake:          make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
	}
}

// Start begins the cleaner goroutine
func (c *Cleaner) Start() {
	c.started.Store(true)
	go c.run()
}

// Stop signals the cleaner to shut down and waits for it to exit. Safe to
// call even if Start was never invoked
func (c *Cleaner) Stop() {
	c.cancel()
	if c.started.Load() {
		<-c.done
	}
}

// Enqueue requests cleanup of all chunks covered by upToRevision. Safe to
// call from any goroutine.
func (c *Cleaner) Enqueue(upToRevision int64) {
	c.mu.Lock()
	if upToRevision <= c.lastCleaned || upToRevision <= c.pendingRevision {
		c.mu.Unlock()
		return
	}
	c.pendingRevision = upToRevision
	if c.metrics != nil {
		c.metrics.CleanupQueuedRevision.Set(float64(upToRevision))
	}
	c.mu.Unlock()

	select {
	case c.wake <- struct{}{}:
	default:
		// A wake is already pending; the loop will pick up the latest
		// pendingRevision when it next runs.
	}
}

// LastCleaned returns the highest revision fully cleaned so far.
func (c *Cleaner) LastCleaned() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastCleaned
}

// run is the main cleaner loop.
func (c *Cleaner) run() {
	defer close(c.done)
	c.logger.Info("chunk cleanup worker started")

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("chunk cleanup worker stopping")
			return
		case <-c.wake:
			for {
				rev, ok := c.takePending()
				if !ok {
					break
				}
				c.performCleanup(rev)
				if c.ctx.Err() != nil {
					return
				}
			}
		}
	}
}

// takePending atomically reads and clears pendingRevision, reporting
// whether there was a revision to take
func (c *Cleaner) takePending() (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingRevision == 0 {
		return 0, false
	}
	rev := c.pendingRevision
	c.pendingRevision = 0
	return rev, true
}

// performCleanup lists and deletes, in bounded rate-limited batches, every
// chunk object covered by upToRevision (revision <= upToRevision). It never
// runs on the snapshot-creation goroutine, so snapshot upload is never
// blocked by it
func (c *Cleaner) performCleanup(upToRevision int64) {
	start := time.Now()
	c.logger.Info("chunk cleanup starting", "up_to_revision", upToRevision)

	chunks, err := datastore.ListChunksForCleanup(c.ctx, c.storageClient, upToRevision)
	if err != nil {
		c.logger.Error("chunk cleanup: failed to list chunks", "up_to_revision", upToRevision, "error", err)
		c.observeCleanup("error", start)
		return
	}

	if c.metrics != nil {
		c.metrics.ChunksListed.Add(float64(len(chunks)))
	}

	batchSize := c.config.Snapshot.CleanupBatchSize
	if batchSize <= 0 {
		batchSize = 250
	}
	batchInterval := c.config.Snapshot.CleanupBatchInterval.Duration

	deleted := 0
	failed := 0

	for i := 0; i < len(chunks); i += batchSize {
		if c.ctx.Err() != nil {
			c.logger.Info("chunk cleanup interrupted by shutdown",
				"up_to_revision", upToRevision, "deleted", deleted, "listed", len(chunks))
			c.observeCleanup("interrupted", start)
			return
		}

		end := min(i+batchSize, len(chunks))
		batch := chunks[i:end]

		keys := make([]string, len(batch))
		for j, chunk := range batch {
			keys[j] = chunk.Key
		}

		failedKeys, err := c.storageClient.DeleteBatch(c.ctx, keys)
		if err != nil {
			c.logger.Warn("chunk cleanup: delete batch failed", "batch_size", len(batch), "error", err)
			failed += len(batch)
			if c.metrics != nil {
				c.metrics.ChunksFailed.Add(float64(len(batch)))
			}
		} else {
			failedSet := make(map[string]struct{}, len(failedKeys))
			for _, k := range failedKeys {
				failedSet[k] = struct{}{}
			}
			for _, key := range keys {
				if _, isFailed := failedSet[key]; isFailed {
					failed++
					if c.metrics != nil {
						c.metrics.ChunksFailed.Inc()
					}
					continue
				}
				deleted++
				if c.metrics != nil {
					c.metrics.ChunksDeleted.Inc()
				}
			}
		}

		if end < len(chunks) && batchInterval > 0 {
			select {
			case <-c.ctx.Done():
				c.logger.Info("chunk cleanup interrupted by shutdown",
					"up_to_revision", upToRevision, "deleted", deleted, "listed", len(chunks))
				c.observeCleanup("interrupted", start)
				return
			case <-time.After(batchInterval):
			}
		}
	}

	c.mu.Lock()
	c.lastCleaned = upToRevision
	c.mu.Unlock()

	if c.metrics != nil {
		c.metrics.CleanupLastRevision.Set(float64(upToRevision))
	}

	c.logger.Info("chunk cleanup completed",
		"up_to_revision", upToRevision,
		"listed_chunks", len(chunks),
		"deleted_chunks", deleted,
		"failed_chunks", failed,
		"duration", time.Since(start),
	)

	c.observeCleanup("success", start)
}

// observeCleanup records cleanup run metrics when metrics are configured.
func (c *Cleaner) observeCleanup(result string, start time.Time) {
	if c.metrics == nil {
		return
	}
	c.metrics.CleanupRuns.WithLabelValues(result).Inc()
	c.metrics.CleanupDur.WithLabelValues(result).Observe(time.Since(start).Seconds())
}
