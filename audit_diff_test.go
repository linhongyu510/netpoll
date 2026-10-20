package netpoll

// AUDIT-ONLY differential property test (not part of upstream PR).
// Models LinkBuffer as a contiguous byte slice and cross-checks read-framing ops.

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestAuditGrowingPeek(t *testing.T) {
	LinkBufferCap = 128
	buf := NewLinkBuffer()
	var model bytes.Buffer
	// multi-node payload
	for i := 0; i < 20; i++ {
		b := []byte{byte('a' + i%26), byte('a' + (i*7)%26), '\n', byte('z')}
		buf.WriteBinary(b)
		model.Write(b)
	}
	buf.Flush()
	exp := model.Bytes()
	// grow Peek n from 1 to total, verifying prefix each time (cachePeek path)
	for n := 1; n <= len(exp); n++ {
		got, err := buf.Peek(n)
		if err != nil {
			t.Fatalf("Peek(%d) err=%v", n, err)
		}
		if !bytes.Equal(got, exp[:n]) {
			t.Fatalf("Peek(%d)\n got=%q\nwant=%q", n, got, exp[:n])
		}
	}
	if buf.Len() != len(exp) {
		t.Fatalf("growing Peek mutated Len: %d != %d", buf.Len(), len(exp))
	}
}

func TestAuditSliceModel(t *testing.T) {
	LinkBufferCap = 128
	buf := NewLinkBuffer()
	var model bytes.Buffer
	for i := 0; i < 20; i++ {
		b := []byte{byte('a' + i%26), byte('b' + i%26), byte('c' + i%26)}
		buf.WriteBinary(b)
		model.Write(b)
	}
	buf.Flush()
	exp := model.Bytes()

	// Slice off first 10 bytes, read them via the slice reader.
	sr, err := buf.Slice(10)
	if err != nil {
		t.Fatalf("Slice err=%v", err)
	}
	got, err := sr.ReadBinary(10)
	if err != nil {
		t.Fatalf("slice ReadBinary err=%v", err)
	}
	if !bytes.Equal(got, exp[:10]) {
		t.Fatalf("slice read\n got=%q\nwant=%q", got, exp[:10])
	}
	// remaining in parent should be exp[10:]
	if buf.Len() != len(exp)-10 {
		t.Fatalf("parent Len=%d want %d", buf.Len(), len(exp)-10)
	}
	if r := buf.Bytes(); !bytes.Equal(r, exp[10:]) {
		t.Fatalf("parent Bytes\n got=%q\nwant=%q", r, exp[10:])
	}
}

func TestAuditLinkBufferDifferential(t *testing.T) {
	LinkBufferCap = 128 // force multi-node splits

	rng := rand.New(rand.NewSource(20261008))

	for trial := 0; trial < 200; trial++ {
		buf := NewLinkBuffer()

		// Build a known payload of varied chunk sizes.
		var model bytes.Buffer
		chunks := 1 + rng.Intn(12)
		for c := 0; c < chunks; c++ {
			sz := 1 + rng.Intn(300)
			chunk := make([]byte, sz)
			for i := range chunk {
				// bias so we often embed newlines for Until
				if rng.Intn(8) == 0 {
					chunk[i] = '\n'
				} else {
					chunk[i] = byte('a' + rng.Intn(26))
				}
			}
			n, err := buf.WriteBinary(chunk)
			if err != nil || n != sz {
				t.Fatalf("trial %d: WriteBinary n=%d err=%v", trial, n, err)
			}
			model.Write(chunk)
		}
		if err := buf.Flush(); err != nil {
			t.Fatalf("trial %d: Flush err=%v", trial, err)
		}

		exp := model.Bytes()

		// Invariant 1: Len + Bytes match model.
		if buf.Len() != len(exp) {
			t.Fatalf("trial %d: Len=%d want %d", trial, buf.Len(), len(exp))
		}
		if got := buf.Bytes(); !bytes.Equal(got, exp) {
			t.Fatalf("trial %d: Bytes mismatch\n got=%q\nwant=%q", trial, got, exp)
		}

		// Interleave reads against the model.
		cur := append([]byte(nil), exp...)
		ops := 1 + rng.Intn(30)
		for o := 0; o < ops && len(cur) > 0; o++ {
			switch rng.Intn(6) {
			case 0: // Next(n)
				n := 1 + rng.Intn(len(cur))
				got, err := buf.Next(n)
				if err != nil {
					t.Fatalf("trial %d op %d: Next(%d) err=%v (len=%d)", trial, o, n, err, len(cur))
				}
				if !bytes.Equal(got, cur[:n]) {
					t.Fatalf("trial %d op %d: Next(%d)\n got=%q\nwant=%q", trial, o, n, got, cur[:n])
				}
				cur = cur[n:]
			case 1: // Peek(n)
				n := 1 + rng.Intn(len(cur))
				got, err := buf.Peek(n)
				if err != nil {
					t.Fatalf("trial %d op %d: Peek(%d) err=%v (len=%d)", trial, o, n, err, len(cur))
				}
				if !bytes.Equal(got, cur[:n]) {
					t.Fatalf("trial %d op %d: Peek(%d)\n got=%q\nwant=%q", trial, o, n, got, cur[:n])
				}
			case 2: // Skip(n)
				n := 1 + rng.Intn(len(cur))
				if err := buf.Skip(n); err != nil {
					t.Fatalf("trial %d op %d: Skip(%d) err=%v (len=%d)", trial, o, n, err, len(cur))
				}
				cur = cur[n:]
			case 3: // Until('\n')
				idx := bytes.IndexByte(cur, '\n')
				if idx < 0 {
					// should error
					if _, err := buf.Until('\n'); err == nil {
						t.Fatalf("trial %d op %d: Until expected error but nil", trial, o)
					}
					continue
				}
				got, err := buf.Until('\n')
				if err != nil {
					t.Fatalf("trial %d op %d: Until err=%v (idx=%d)", trial, o, err, idx)
				}
				want := cur[:idx+1]
				if !bytes.Equal(got, want) {
					t.Fatalf("trial %d op %d: Until\n got=%q\nwant=%q", trial, o, got, want)
				}
				cur = cur[idx+1:]
			case 4: // ReadString(n)
				n := 1 + rng.Intn(len(cur))
				got, err := buf.ReadString(n)
				if err != nil {
					t.Fatalf("trial %d op %d: ReadString(%d) err=%v (len=%d)", trial, o, n, err, len(cur))
				}
				if got != string(cur[:n]) {
					t.Fatalf("trial %d op %d: ReadString(%d)\n got=%q\nwant=%q", trial, o, n, got, cur[:n])
				}
				cur = cur[n:]
			case 5: // ReadByte
				b, err := buf.ReadByte()
				if err != nil {
					t.Fatalf("trial %d op %d: ReadByte err=%v (len=%d)", trial, o, err, len(cur))
				}
				if b != cur[0] {
					t.Fatalf("trial %d op %d: ReadByte got=%q want=%q", trial, o, []byte{b}, cur[:1])
				}
				cur = cur[1:]
			}

			// Re-assert invariants after every op.
			if buf.Len() != len(cur) {
				t.Fatalf("trial %d op %d: post-op Len=%d want %d", trial, o, buf.Len(), len(cur))
			}
			if got := buf.Bytes(); !bytes.Equal(got, cur) {
				t.Fatalf("trial %d op %d: post-op Bytes mismatch\n got=%q\nwant=%q", trial, o, got, cur)
			}
			_ = buf.Release()
		}
	}
}
