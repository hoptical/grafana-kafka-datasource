package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

const (
	// defaultMicroBatchMaxRows bounds how many rows are grouped before an
	// immediate flush. Keeping this small limits added latency while still
	// amortizing frame serialization cost.
	defaultMicroBatchMaxRows = 32

	// defaultMicroBatchMaxLatency bounds the time a row can wait before
	// being sent when traffic is low. This is used by RunStream's flush ticker.
	defaultMicroBatchMaxLatency = 5 * time.Millisecond
)

type frameMicroBatcher struct {
	maxRows     int
	pendingRows int
	pending     map[string]*data.Frame
	order       []string
}

func newFrameMicroBatcher(maxRows int) *frameMicroBatcher {
	if maxRows < 1 {
		maxRows = 1
	}
	return &frameMicroBatcher{
		maxRows: maxRows,
		pending: make(map[string]*data.Frame),
		order:   make([]string, 0, 8),
	}
}

func (b *frameMicroBatcher) AddFrames(frames []*data.Frame) ([]*data.Frame, error) {
	for _, frame := range frames {
		if frame == nil {
			continue
		}
		rows := frame.Rows()
		if rows == 0 {
			continue
		}

		key, err := frameSchemaKey(frame)
		if err != nil {
			return nil, err
		}
		agg, exists := b.pending[key]
		if !exists {
			agg = frame.EmptyCopy()
			agg.Meta = frame.Meta
			for i := range agg.Fields {
				agg.Fields[i].Config = frame.Fields[i].Config
				if len(frame.Fields[i].Labels) > 0 {
					labels := make(data.Labels, len(frame.Fields[i].Labels))
					for k, v := range frame.Fields[i].Labels {
						labels[k] = v
					}
					agg.Fields[i].Labels = labels
				}
			}
			b.pending[key] = agg
			b.order = append(b.order, key)
		}
		if err := appendFrameRows(agg, frame); err != nil {
			return nil, err
		}
		b.pendingRows += rows
	}

	if b.pendingRows >= b.maxRows {
		return b.Flush(), nil
	}
	return nil, nil
}

func (b *frameMicroBatcher) Flush() []*data.Frame {
	if b.pendingRows == 0 {
		return nil
	}
	out := make([]*data.Frame, 0, len(b.order))
	for _, key := range b.order {
		if f := b.pending[key]; f != nil && f.Rows() > 0 {
			out = append(out, f)
		}
		delete(b.pending, key)
	}
	b.order = b.order[:0]
	b.pendingRows = 0
	return out
}

func appendFrameRows(dst, src *data.Frame) error {
	if dst == nil || src == nil {
		return errors.New("nil frame")
	}
	if dst.Name != src.Name {
		return fmt.Errorf("frame name mismatch: dst=%q src=%q", dst.Name, src.Name)
	}
	if dst.RefID != src.RefID {
		return fmt.Errorf("frame refid mismatch: dst=%q src=%q", dst.RefID, src.RefID)
	}
	if len(dst.Fields) != len(src.Fields) {
		return fmt.Errorf("field count mismatch: dst=%d src=%d", len(dst.Fields), len(src.Fields))
	}
	for i := range dst.Fields {
		df := dst.Fields[i]
		sf := src.Fields[i]
		if df == nil || sf == nil {
			return errors.New("nil field")
		}
		if df.Name != sf.Name {
			return fmt.Errorf("field name mismatch at index %d: dst=%q src=%q", i, df.Name, sf.Name)
		}
		if df.Type() != sf.Type() {
			return fmt.Errorf("field type mismatch at index %d: dst=%v src=%v", i, df.Type(), sf.Type())
		}
		if !reflect.DeepEqual(df.Config, sf.Config) {
			return fmt.Errorf("field config mismatch at index %d", i)
		}
		if !labelsEqual(df.Labels, sf.Labels) {
			return fmt.Errorf("field labels mismatch at index %d", i)
		}
	}

	rows := src.Rows()
	for row := 0; row < rows; row++ {
		dst.AppendRow(src.RowCopy(row)...)
	}
	return nil
}

func frameSchemaKey(frame *data.Frame) (string, error) {
	if frame == nil {
		return "", errors.New("nil frame")
	}
	if len(frame.Fields) == 0 {
		return "", errors.New("frame has no fields")
	}

	var b strings.Builder
	b.Grow(128)
	b.WriteString(frame.Name)
	b.WriteByte('|')
	b.WriteString(frame.RefID)
	b.WriteByte('|')
	b.WriteString(strconv.Itoa(len(frame.Fields)))
	b.WriteByte('|')
	if frame.Meta != nil {
		metaSig, err := valueSignature(frame.Meta)
		if err != nil {
			return "", fmt.Errorf("frame meta signature: %w", err)
		}
		b.WriteString("meta:")
		b.WriteString(metaSig)
		b.WriteByte('|')
	}

	for _, field := range frame.Fields {
		if field == nil {
			return "", errors.New("frame has nil field")
		}
		b.WriteString(field.Name)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(int(field.Type())))
		if len(field.Labels) > 0 {
			b.WriteByte(':')
			b.WriteString("labels=")
			appendLabelsKey(&b, field.Labels)
		}
		if field.Config != nil {
			sig, err := valueSignature(field.Config)
			if err != nil {
				return "", fmt.Errorf("field config signature: %w", err)
			}
			b.WriteByte(':')
			b.WriteString("cfg=")
			b.WriteString(sig)
		}
		b.WriteByte('|')
	}

	return b.String(), nil
}

func appendLabelsKey(b *strings.Builder, labels data.Labels) {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte(',')
	}
}

func labelsEqual(a, b data.Labels) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func valueSignature(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return strconv.FormatUint(h.Sum64(), 16), nil
}
