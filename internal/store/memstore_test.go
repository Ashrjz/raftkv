package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	s := NewMemStore()

	if _, err := s.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get("a")
	if err != nil || string(got) != "1" {
		t.Fatalf("got %q, %v", got, err)
	}

	s.Put("a", []byte("2")) // overwrite
	got, _ = s.Get("a")
	if string(got) != "2" {
		t.Fatalf("overwrite failed: %q", got)
	}

	s.Delete("a")
	if _, err := s.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
	if err := s.Delete("a"); err != nil { // idempotent
		t.Fatalf("delete of missing key errored: %v", err)
	}
}

func TestNoAliasing(t *testing.T) {
	s := NewMemStore()
	in := []byte("hello")
	s.Put("k", in)
	in[0] = 'X' // mutate caller's slice after Put

	out, _ := s.Get("k")
	if string(out) != "hello" {
		t.Fatalf("store was mutated via input slice: %q", out)
	}
	out[0] = 'Y' // mutate returned slice
	again, _ := s.Get("k")
	if string(again) != "hello" {
		t.Fatalf("store was mutated via output slice: %q", again)
	}
}

func TestConcurrent(t *testing.T) {
	s := NewMemStore()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				k := fmt.Sprintf("k%d", i%50)
				s.Put(k, []byte{byte(g)})
				s.Get(k)
				if i%10 == 0 {
					s.Delete(k)
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkPut(b *testing.B) {
	s := NewMemStore()
	v := []byte("value")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Put(fmt.Sprintf("k%d", i%1000), v)
			i++
		}
	})
}
