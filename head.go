// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/syncmap"

	"encoding/binary"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

var (
	// ErrNotFound is returned if a looked up resource was not found.
	ErrNotFound = errors.Errorf("not found")

	// ErrOutOfOrderSample is returned if an appended sample has a
	// timestamp larger than the most recent sample.
	ErrOutOfOrderSample = errors.New("out of order sample")

	// ErrAmendSample is returned if an appended sample has the same timestamp
	// as the most recent sample but a different value.
	ErrAmendSample = errors.New("amending sample")

	// ErrOutOfBounds is returned if an appended sample is out of the
	// writable time range.
	ErrOutOfBounds = errors.New("out of bounds")
)

// HeadBlock handles reads and writes of time series data within a time window.
type HeadBlock struct {
	dir       string
	wal       WAL
	compactor Compactor
	isolation *Isolation

	lastSeriesID  uint32
	activeWriters uint64
	highTimestamp int64
	stopc         chan struct{}

	// descs holds all chunk descs for the head block. Each chunk implicitly
	// is assigned the index as its ID.
	// series []*memSeries
	// hashes contains a collision map of label set hashes of chunks
	// to their chunk descs.
	// hashes map[uint64][]*memSeries
	series  syncmap.Map
	hashes  syncmap.Map
	symbols syncmap.Map

	invIndex *invertedIndexLSM

	tombstones tombstoneReader

	meta BlockMeta
}

// TouchHeadBlock atomically touches a new head block in dir for
// samples in the range [mint,maxt).
func TouchHeadBlock(dir string, mint, maxt int64) (string, error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))

	ulid, err := ulid.New(ulid.Now(), entropy)
	if err != nil {
		return "", err
	}

	// Make head block creation appear atomic.
	dir = filepath.Join(dir, ulid.String())
	tmp := dir + ".tmp"

	if err := os.MkdirAll(tmp, 0777); err != nil {
		return "", err
	}

	if err := writeMetaFile(tmp, &BlockMeta{
		ULID:    ulid,
		MinTime: mint,
		MaxTime: maxt,
	}); err != nil {
		return "", err
	}

	return dir, renameFile(tmp, dir)
}

// OpenHeadBlock opens the head block in dir.
func OpenHeadBlock(dir string, l log.Logger, isolation *Isolation, wal WAL, c Compactor) (*HeadBlock, error) {
	meta, err := readMetaFile(dir)
	if err != nil {
		return nil, err
	}
	if isolation == nil {
		isolation = NewIsolation()
	}
	stopc := make(chan struct{})

	h := &HeadBlock{
		dir:        dir,
		wal:        wal,
		invIndex:   newInvertedIndexLSM(stopc),
		compactor:  c,
		isolation:  isolation,
		meta:       *meta,
		tombstones: newEmptyTombstoneReader(),
		stopc:      stopc,
	}
	return h, h.init()
}

func (h *HeadBlock) init() error {
	rev, _ := h.isolation.StartWrite()
	defer h.isolation.WriteDone(rev)

	r := h.wal.Reader()

	hasher := sha256.New()

	seriesFunc := func(series []labels.Labels) error {
		for _, lset := range series {
			hasher.Reset()
			var hash [sha256.Size]byte
			hashLabels(hasher, lset, hash[:0])

			h.createOrGetSeries(hash, lset, rev)
			h.meta.Stats.NumSeries++
		}

		return nil
	}
	samplesFunc := func(samples []RefSample) error {
		for _, s := range samples {
			series, ok := h.seriesByID(uint32(s.Ref))
			if !ok {
				return errors.Wrapf(ErrNotFound, "unknown series ID %d; abort WAL restore", s.Ref)
			}
			if !h.inBounds(s.T) {
				return errors.Wrap(ErrOutOfBounds, "consume WAL")
			}

			series.append(s.T, s.V)
			h.meta.Stats.NumSamples++
		}

		return nil
	}
	deletesFunc := func(stones []Stone) error {
		for _, s := range stones {
			for _, itv := range s.intervals {
				h.tombstones.add(s.ref, itv)
			}
		}

		return nil
	}

	if err := r.Read(seriesFunc, samplesFunc, deletesFunc); err != nil {
		return errors.Wrap(err, "consume WAL")
	}

	return nil
}

// inBounds returns true if the given timestamp is within the valid
// time bounds of the block.
func (h *HeadBlock) inBounds(t int64) bool {
	return t >= h.meta.MinTime && t <= h.meta.MaxTime
}

func (h *HeadBlock) String() string {
	return h.meta.ULID.String()
}

// Close syncs all data and closes underlying resources of the head block.
func (h *HeadBlock) Close() error {
	select {
	case <-h.stopc:
		return errors.Errorf("already closed")
	default:
	}
	close(h.stopc)

	if err := h.wal.Close(); err != nil {
		return errors.Wrapf(err, "close WAL for head %s", h.dir)
	}
	// Check whether the head block still exists in the underlying dir
	// or has already been replaced with a compacted version or removed.
	meta, err := readMetaFile(h.dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if meta.ULID == h.meta.ULID {
		return writeMetaFile(h.dir, &h.meta)
	}
	return nil
}

// Meta returns a BlockMeta for the head block.
func (h *HeadBlock) Meta() BlockMeta {
	m := BlockMeta{
		ULID:       h.meta.ULID,
		MinTime:    h.meta.MinTime,
		MaxTime:    h.meta.MaxTime,
		Compaction: h.meta.Compaction,
	}

	m.Stats.NumChunks = atomic.LoadUint64(&h.meta.Stats.NumChunks)
	m.Stats.NumSeries = atomic.LoadUint64(&h.meta.Stats.NumSeries)
	m.Stats.NumSamples = atomic.LoadUint64(&h.meta.Stats.NumSamples)

	return m
}

// Tombstones returns the TombstoneReader against the block.
func (h *HeadBlock) Tombstones() TombstoneReader {
	return h.tombstones
}

// Delete implements headBlock.
func (h *HeadBlock) Delete(mint int64, maxt int64, ms ...labels.Matcher) error {
	ir := h.Index()

	pr := newPostingsReader(ir)
	p, absent := pr.Select(ms...)

	var stones []Stone

Outer:
	for p.Next() {
		ref := p.At()

		s, ok := h.seriesByID(ref)
		if !ok {
			return errors.Wrapf(ErrNotFound, "invalid series ID %d in postings", ref)
		}
		for _, abs := range absent {
			if s.lset.Get(abs) != "" {
				continue Outer
			}
		}

		// Delete only until the current values and not beyond.
		tmin, tmax := clampInterval(mint, maxt, s.chunks[0].minTime, s.head().maxTime)
		stones = append(stones, Stone{ref, Intervals{{tmin, tmax}}})
	}

	if p.Err() != nil {
		return p.Err()
	}
	if err := h.wal.LogDeletes(stones); err != nil {
		return err
	}

	for _, s := range stones {
		h.tombstones.add(s.ref, s.intervals[0])
	}

	h.meta.Stats.NumTombstones = uint64(len(h.tombstones))
	return nil
}

// Snapshot persists the current state of the headblock to the given directory.
// Callers must ensure that there are no active appenders against the block.
// DB does this by acquiring its own write lock.
func (h *HeadBlock) Snapshot(snapshotDir string) error {
	if h.meta.Stats.NumSeries == 0 {
		return nil
	}
	return h.compactor.Write(snapshotDir, h)
}

// Dir returns the directory of the block.
func (h *HeadBlock) Dir() string { return h.dir }

// Index returns an IndexReader against the block.
func (h *HeadBlock) Index() IndexReader {
	rev := h.isolation.StartRead()

	r := h.indexReader(rev)
	r.close = func() { h.isolation.ReadDone(rev) }

	return r
}

func (h *HeadBlock) indexReader(rev uint64) *headIndexReader {
	return &headIndexReader{block: h, revision: rev, close: func() {}}
}

// Chunks returns a ChunkReader against the block.
func (h *HeadBlock) Chunks() ChunkReader { return &headChunkReader{h} }

// Querier returns a new Querier against the block for the range [mint, maxt].
func (h *HeadBlock) Querier(mint, maxt int64) Querier {
	select {
	case <-h.stopc:
		panic(fmt.Sprintf("block %s already closed", h.dir))
	default:
	}

	rev := h.isolation.StartRead()

	// TODO(fabxc): isolate chunk and tombstone readers as well.
	return &blockQuerier{
		mint:       mint,
		maxt:       maxt,
		index:      h.indexReader(rev),
		chunks:     h.Chunks(),
		tombstones: h.Tombstones(),
		close:      func() { h.isolation.ReadDone(rev) },
	}
}

// Appender returns a new Appender against the head block.
func (h *HeadBlock) Appender() Appender {
	select {
	case <-h.stopc:
		panic(fmt.Sprintf("block %s already closed", h.dir))
	default:
	}

	atomic.AddUint64(&h.activeWriters, 1)

	curRev, minRev := h.isolation.StartWrite()

	return &headAppender{
		block:           h,
		revision:        curRev,
		minReadRevision: minRev,
		samples:         getHeadAppendBuffer(),
	}
}

// ActiveWriters returns true if the block has open write transactions.
func (h *HeadBlock) ActiveWriters() int {
	return int(atomic.LoadUint64(&h.activeWriters))
}

// HighTimestamp returns the highest inserted sample timestamp.
func (h *HeadBlock) HighTimestamp() int64 {
	return atomic.LoadInt64(&h.highTimestamp)
}

var headPool = sync.Pool{}

func getHeadAppendBuffer() []RefSample {
	b := headPool.Get()
	if b == nil {
		return make([]RefSample, 0, 512)
	}
	return b.([]RefSample)
}

func putHeadAppendBuffer(b []RefSample) {
	headPool.Put(b[:0])
}

type headAppender struct {
	block                     *HeadBlock
	revision, minReadRevision uint64
	hasher                    hash.Hash

	newSeries []*hashedLabels
	newLabels []labels.Labels
	newHashes map[[sha256.Size]byte]uint64

	samples       []RefSample
	highTimestamp int64
}

type hashedLabels struct {
	ref    uint64
	hash   [sha256.Size]byte
	labels labels.Labels
}

func hashLabels(h hash.Hash, lset labels.Labels, b []byte) {
	h.Reset()

	for _, l := range lset {
		h.Write(yoloBytes(l.Name))
		h.Write([]byte{'\xff'})
		h.Write(yoloBytes(l.Value))
		h.Write([]byte{'\xff'})
	}
	h.Sum(b)
}

func (a *headAppender) Add(lset labels.Labels, t int64, v float64) (string, error) {
	if !a.block.inBounds(t) {
		return "", ErrOutOfBounds
	}
	if a.hasher == nil {
		a.hasher = sha256.New()
	}
	var hash [sha256.Size]byte
	hashLabels(a.hasher, lset, hash[:0])

	refb := make([]byte, 8)

	// Series exists already in the block.
	if ms, ok := a.block.seriesByHash(hash); ok {
		binary.BigEndian.PutUint64(refb, uint64(ms.ref))
		return string(refb), a.AddFast(string(refb), t, v)
	}
	// Series was added in this transaction previously.
	if ref, ok := a.newHashes[hash]; ok {
		binary.BigEndian.PutUint64(refb, ref)
		// XXX(fabxc): there's no fast path for multiple samples for the same new series
		// in the same transaction. We always return the invalid empty ref. It's has not
		// been a relevant use case so far and is not worth the trouble.
		return "", a.AddFast(string(refb), t, v)
	}

	// The series is completely new.
	if a.newSeries == nil {
		a.newHashes = map[[sha256.Size]byte]uint64{}
	}
	// First sample for new series.
	ref := uint64(len(a.newSeries))

	a.newSeries = append(a.newSeries, &hashedLabels{
		ref:    ref,
		hash:   hash,
		labels: lset,
	})
	// First bit indicates its a series created in this transaction.
	ref |= (1 << 63)

	a.newHashes[hash] = ref
	binary.BigEndian.PutUint64(refb, ref)

	return "", a.AddFast(string(refb), t, v)
}

func (a *headAppender) AddFast(ref string, t int64, v float64) error {
	if len(ref) != 8 {
		return errors.Wrap(ErrNotFound, "invalid ref length")
	}
	var (
		refn = binary.BigEndian.Uint64(yoloBytes(ref))
		id   = uint32(refn)
		inTx = refn&(1<<63) != 0
	)
	// Distinguish between existing series and series created in
	// this transaction.
	if inTx {
		if id > uint32(len(a.newSeries)-1) {
			return errors.Wrap(ErrNotFound, "transaction series ID too high")
		}
		// TODO(fabxc): we also have to validate here that the
		// sample sequence is valid.
		// We also have to revalidate it as we switch locks and create
		// the new series.
	} else {
		ms, ok := a.block.seriesByID(id)
		if !ok {
			return errors.Wrapf(ErrNotFound, "invalid series ID %d", id)
		}
		// TODO(fabxc): memory series should be locked here already.
		// Only problem is release of locks in case of a rollback.
		c := ms.head()

		if !a.block.inBounds(t) {
			return ErrOutOfBounds
		}
		if t < c.maxTime {
			return ErrOutOfOrderSample
		}

		// We are allowing exact duplicates as we can encounter them in valid cases
		// like federation and erroring out at that time would be extremely noisy.
		if c.maxTime == t && math.Float64bits(ms.lastValue) != math.Float64bits(v) {
			return ErrAmendSample
		}
	}

	if t > a.highTimestamp {
		a.highTimestamp = t
	}

	a.samples = append(a.samples, RefSample{
		Ref: refn,
		T:   t,
		V:   v,
	})
	return nil
}

// createSeries creates any new series of the transaction and writes them to the write ahead log.
func (a *headAppender) createSeries() error {
	if len(a.newSeries) == 0 {
		return nil
	}

	ivx := newInvertedIndex()
	newLabels := make([]labels.Labels, 0, len(a.newSeries))

	for _, hl := range a.newSeries {
		s, created := a.block.createOrGetSeries(hl.hash, hl.labels, a.revision)
		if created {
			newLabels = append(newLabels, hl.labels)
		}
		hl.ref = uint64(s.ref)

		for _, l := range hl.labels {
			a.block.symbols.LoadOrStore(l.Name, struct{}{})
			a.block.symbols.LoadOrStore(l.Value, struct{}{})
		}

		ivx.add(s.ref, hl.labels)
	}

	if err := a.block.wal.LogSeries(a.newLabels); err != nil {
		return errors.Wrap(err, "WAL log series")
	}
	a.block.invIndex.add(ivx)

	return nil
}

func (h *HeadBlock) seriesByID(id uint32) (*memSeries, bool) {
	s, ok := h.series.Load(id)
	if ok {
		return s.(*memSeries), true
	}
	return nil, false
}

// get retrieves the chunk with the hash and label set and creates
// a new one if it doesn't exist yet.
func (h *HeadBlock) seriesByHash(hash [sha256.Size]byte) (*memSeries, bool) {
	s, ok := h.hashes.Load(hash)
	if ok {
		return s.(*memSeries), true
	}
	return nil, false
}

// createOrGetSeries atomically creates a series for the given label set or returns it if
// it already exists.
func (h *HeadBlock) createOrGetSeries(hash [sha256.Size]byte, lset labels.Labels, revision uint64) (*memSeries, bool) {
	s := newMemSeries(lset, 0, h.meta.MaxTime)

	actual, ok := h.hashes.LoadOrStore(hash, s)
	if ok {
		return actual.(*memSeries), false
	}
	// Series was newly created, assign it the next ID.
	s.ref = atomic.AddUint32(&h.lastSeriesID, 1)
	s.rev = revision
	// If incrementing brought us back to 0, we exceeded the uint32.
	if s.ref == 0 {
		panic("series ID overflow")
	}
	h.series.Store(s.ref, s)

	return s, true
}

func (a *headAppender) Commit() error {
	if err := a.createSeries(); err != nil {
		return err
	}

	// We have to update the refs of samples for series we just created.
	for i := range a.samples {
		s := &a.samples[i]
		if s.Ref&(1<<63) != 0 {
			s.Ref = a.newSeries[(s.Ref<<1)>>1].ref
		}
	}

	// Write all new samples to the WAL and add them to the
	// in-mem database on success.
	if err := a.block.wal.LogSamples(a.samples); err != nil {
		return errors.Wrap(err, "WAL log samples")
	}

	total := uint64(len(a.samples))

	for _, s := range a.samples {
		series, ok := a.block.seriesByID(uint32(s.Ref))
		if !ok {
			return errors.Errorf("series with ID %d not found", s.Ref)
		}
		if !series.append(s.T, s.V) {
			total--
		}
	}

	atomic.AddUint64(&a.block.meta.Stats.NumSamples, total)
	atomic.AddUint64(&a.block.meta.Stats.NumSeries, uint64(len(a.newSeries)))

	for {
		ht := a.block.HighTimestamp()
		if a.highTimestamp <= ht {
			break
		}
		if atomic.CompareAndSwapInt64(&a.block.highTimestamp, ht, a.highTimestamp) {
			break
		}
	}

	return a.Rollback()
}

func (a *headAppender) Rollback() error {
	atomic.AddUint64(&a.block.activeWriters, ^uint64(0))
	putHeadAppendBuffer(a.samples)

	a.block.isolation.WriteDone(a.revision)
	return nil
}

type headChunkReader struct {
	block *HeadBlock
}

// Chunk returns the chunk for the reference number.
func (h *headChunkReader) Chunk(ref uint64) (chunks.Chunk, error) {
	sid := uint32(ref >> 32)
	cid := uint32(ref)

	series, ok := h.block.seriesByID(sid)
	if !ok {
		return nil, errors.Errorf("series with ID %d not found", sid)
	}

	c := &safeChunk{
		Chunk: series.chunks[cid].chunk,
		s:     series,
		i:     int(cid),
	}
	return c, nil
}

func (h *headChunkReader) Close() error {
	return nil
}

type safeChunk struct {
	chunks.Chunk
	s *memSeries
	i int
}

func (c *safeChunk) Iterator() chunks.Iterator {
	c.s.mtx.RLock()
	defer c.s.mtx.RUnlock()
	return c.s.iterator(c.i)
}

// func (c *safeChunk) Appender() (chunks.Appender, error) { panic("illegal") }
// func (c *safeChunk) Bytes() []byte                      { panic("illegal") }
// func (c *safeChunk) Encoding() chunks.Encoding          { panic("illegal") }

type headIndexReader struct {
	block    *HeadBlock
	revision uint64
	close    func()
}

func (h *headIndexReader) Close() error {
	h.close()
	return nil
}

func (h *headIndexReader) Symbols() (map[string]struct{}, error) {
	res := map[string]struct{}{}

	h.block.symbols.Range(func(k, _ interface{}) bool {
		res[k.(string)] = struct{}{}
		return true
	})
	return res, nil
}

// LabelValues returns the possible label values
func (h *headIndexReader) LabelValues(names ...string) (StringTuples, error) {
	if len(names) != 1 {
		return nil, errInvalidSize
	}
	return &stringTuples{l: len(names), s: h.block.invIndex.values(names[0])}, nil
}

// Postings returns the postings list iterator for the label pair.
func (h *headIndexReader) Postings(name, value string) (Postings, error) {
	return h.block.invIndex.postings(name, value), nil
}

func (h *headIndexReader) SortedPostings(p Postings) Postings {
	ep := make([]uint32, 0, 256)

	for p.Next() {
		ep = append(ep, p.At())
	}
	if err := p.Err(); err != nil {
		return errPostings{err: errors.Wrap(err, "expand postings")}
	}

	var err error

	sort.Slice(ep, func(i, j int) bool {
		if err != nil {
			return false
		}
		a, ok1 := h.block.seriesByID(ep[i])
		b, ok2 := h.block.seriesByID(ep[j])

		if !ok1 || !ok2 {
			err = errors.Errorf("invalid series ID in postings")
			return false
		}
		return labels.Compare(a.lset, b.lset) < 0
	})
	if err != nil {
		return errPostings{err: err}
	}
	return newListPostings(ep)
}

// Series returns the series for the given reference.
func (h *headIndexReader) Series(ref uint32, lbls *labels.Labels, chks *[]ChunkMeta) error {
	s, ok := h.block.seriesByID(ref)
	if !ok || s.rev > h.revision {
		return ErrNotFound
	}

	*lbls = append((*lbls)[:0], s.lset...)

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	*chks = (*chks)[:0]

	for i, c := range s.chunks {
		*chks = append(*chks, ChunkMeta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     (uint64(ref) << 32) | uint64(i),
		})
	}

	return nil
}

func (h *headIndexReader) LabelIndices() ([][]string, error) {
	res := [][]string{}

	for _, s := range h.block.invIndex.names() {
		res = append(res, []string{s})
	}
	return res, nil
}

type sample struct {
	t int64
	v float64
}

type memSeries struct {
	mtx sync.RWMutex

	ref    uint32
	rev    uint64
	lset   labels.Labels
	chunks []*memChunk

	nextAt    int64 // timestamp at which to cut the next chunk.
	maxt      int64 // maximum timestamp for the series.
	lastValue float64
	sampleBuf [4]sample

	app chunks.Appender // Current appender for the chunk.
}

func (s *memSeries) cut(mint int64) *memChunk {
	c := &memChunk{
		chunk:   chunks.NewXORChunk(),
		minTime: mint,
		maxTime: math.MinInt64,
	}
	s.chunks = append(s.chunks, c)

	app, err := c.chunk.Appender()
	if err != nil {
		panic(err)
	}
	s.app = app
	return c
}

func newMemSeries(lset labels.Labels, id uint32, maxt int64) *memSeries {
	s := &memSeries{
		lset:   lset,
		ref:    id,
		maxt:   maxt,
		nextAt: math.MinInt64,
	}
	return s
}

func (s *memSeries) append(t int64, v float64) bool {
	const samplesPerChunk = 120

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var c *memChunk

	if len(s.chunks) == 0 {
		c = s.cut(t)
	}
	c = s.head()
	if c.maxTime >= t {
		return false
	}
	if c.samples > samplesPerChunk/4 && t >= s.nextAt {
		c = s.cut(t)
	}
	s.app.Append(t, v)

	c.maxTime = t
	c.samples++

	if c.samples == samplesPerChunk/4 {
		s.nextAt = computeChunkEndTime(c.minTime, c.maxTime, s.maxt)
	}

	s.lastValue = v

	s.sampleBuf[0] = s.sampleBuf[1]
	s.sampleBuf[1] = s.sampleBuf[2]
	s.sampleBuf[2] = s.sampleBuf[3]
	s.sampleBuf[3] = sample{t: t, v: v}

	return true
}

// computeChunkEndTime estimates the end timestamp based the beginning of a chunk,
// its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
func computeChunkEndTime(start, cur, max int64) int64 {
	a := (max - start) / ((cur - start + 1) * 4)
	if a == 0 {
		return max
	}
	return start + (max-start)/a
}

func (s *memSeries) iterator(i int) chunks.Iterator {
	c := s.chunks[i]

	if i < len(s.chunks)-1 {
		return c.chunk.Iterator()
	}

	it := &memSafeIterator{
		Iterator: c.chunk.Iterator(),
		i:        -1,
		total:    c.samples,
		buf:      s.sampleBuf,
	}
	return it
}

func (s *memSeries) head() *memChunk {
	return s.chunks[len(s.chunks)-1]
}

type memChunk struct {
	chunk            chunks.Chunk
	minTime, maxTime int64
	samples          int
}

type memSafeIterator struct {
	chunks.Iterator

	i     int
	total int
	buf   [4]sample
}

func (it *memSafeIterator) Next() bool {
	if it.i+1 >= it.total {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *memSafeIterator) At() (int64, float64) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.buf[4-(it.total-it.i)]
	return s.t, s.v
}

type invertedIndexLSM struct {
	mtx      sync.RWMutex
	children []*invertedIndex
	signal   chan struct{}
}

func newInvertedIndexLSM(done <-chan struct{}) *invertedIndexLSM {
	ix := &invertedIndexLSM{signal: make(chan struct{})}
	go ix.run(done)
	return ix
}

func (iv *invertedIndexLSM) run(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-iv.signal:
			iv.merge()
		}
	}
}

// merge compacts any existing children
func (iv *invertedIndexLSM) merge() {
	iv.mtx.RLock()
	k := len(iv.children)
	iv.mtx.RUnlock()

	if k < 2 {
		return
	}
	// Create new swap index with the current state that we can merge updates into.
	prim := iv.children[0]
	swap := newInvertedIndex()

	for t, ids := range prim.postings.m {
		swap.postings.m[t] = ids
	}
	for name, vals := range prim.values {
		x := make(stringset, len(vals))
		x.merge(vals)
		swap.values[name] = x
	}

	// Compact children into inactive swap index.
	for _, c := range iv.children[1:k] {
		// Existing IDs are sequential and non-overlapping across children.
		for t, ids := range c.postings.m {
			swap.postings.m[t] = append(swap.postings.m[t], ids...)
		}
		for name, vals := range c.values {
			ss, ok := swap.values[name]
			if !ok {
				ss = make(stringset, len(vals))
				swap.values[name] = ss
			}
			ss.merge(vals)
		}
	}

	// Remove compacted children and make swap index the primary.
	iv.mtx.Lock()
	iv.children[0] = swap
	iv.children = append(iv.children[:1], iv.children[k:]...)
	iv.mtx.Unlock()
}

func (iv *invertedIndexLSM) add(child *invertedIndex) {
	iv.mtx.Lock()
	iv.children = append(iv.children, child)
	iv.mtx.Unlock()

	select {
	case iv.signal <- struct{}{}:
	default:
	}
}

func (iv *invertedIndexLSM) values(name string) []string {
	iv.mtx.RLock()
	defer iv.mtx.RUnlock()

	m := map[string]struct{}{}
	for _, c := range iv.children {
		for s := range c.values[name] {
			m[s] = struct{}{}
		}
	}

	names := make([]string, 0, len(m))
	for s := range m {
		names = append(names, s)
	}
	sort.Strings(names)

	return names
}

func (iv *invertedIndexLSM) names() []string {
	iv.mtx.RLock()
	defer iv.mtx.RUnlock()

	m := map[string]struct{}{}
	for _, c := range iv.children {
		for s := range c.values {
			m[s] = struct{}{}
		}
	}

	names := make([]string, 0, len(m))
	for s := range m {
		names = append(names, s)
	}
	sort.Strings(names)

	return names
}

func (iv *invertedIndexLSM) postings(name, value string) Postings {
	iv.mtx.RLock()
	defer iv.mtx.RUnlock()

	all := make([]Postings, 0, len(iv.children))
	for _, c := range iv.children {
		all = append(all, c.postings.get(term{name, value}))
	}

	return &chainedPostings{postings: all}
}

type invertedIndex struct {
	postings *memPostings
	values   map[string]stringset
}

func newInvertedIndex() *invertedIndex {
	return &invertedIndex{
		postings: &memPostings{m: map[term][]uint32{}},
		values:   map[string]stringset{},
	}
}

func (ix *invertedIndex) add(id uint32, lset labels.Labels) {
	for _, l := range lset {
		valset, ok := ix.values[l.Name]
		if !ok {
			valset = stringset{}
			ix.values[l.Name] = valset
		}
		valset.set(l.Value)

		ix.postings.add(id, term{name: l.Name, value: l.Value})
	}
}
