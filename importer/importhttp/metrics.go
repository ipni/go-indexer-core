package importhttp

import (
	"context"
	"io"

	"github.com/ipni/go-indexer-core/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// Results of a part request, the values of the metrics.ImportResult tag
const (
	resultOK      = "ok"
	resultRefused = "refused"
	resultFailed  = "failed"
	resultAborted = "aborted"
)

// recordResult records the end of a part request with result in measure,
// with the tags of ctx
func recordResult(ctx context.Context, measure *stats.Int64Measure, result string) {
	_ = stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(metrics.ImportResult, result)}, measure.M(1))
}

// countingWriter writes to w and counts the bytes written
type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

// countingReader reads r and counts the bytes read
type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}
