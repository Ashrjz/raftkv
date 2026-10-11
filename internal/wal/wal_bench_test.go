package wal

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func BenchmarkAppend(b *testing.B) {
	modes := []struct {
		name string
		opts []Option
	}{
		{"sync", nil},
		{"group", []Option{WithGroupCommit()}},
	}
	for _, m := range modes {
		for _, g := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("%s/goroutines=%d", m.name, g), func(b *testing.B) {
				w, err := Open(filepath.Join(b.TempDir(), "wal.log"), m.opts...)
				if err != nil {
					b.Fatal(err)
				}
				defer w.Close()

				rec := Record{Type: TypePut, Key: []byte("benchmark-key"), Value: make([]byte, 100)}

				var wg sync.WaitGroup
				b.ResetTimer()
				for i := 0; i < g; i++ {
					n := b.N / g
					if i < b.N%g {
						n++
					}
					wg.Add(1)
					go func(n int) {
						defer wg.Done()
						for j := 0; j < n; j++ {
							if err := w.Append(rec); err != nil {
								b.Error(err)
								return
							}
						}
					}(n)
				}
				wg.Wait()
				b.StopTimer()

				if batches, records := w.Stats(); batches > 0 {
					b.ReportMetric(float64(records)/float64(batches), "records/fsync")
				}
			})
		}
	}
}
