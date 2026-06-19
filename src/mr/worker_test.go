package mr

import (
	"os"
	"strings"
	"testing"
	"unicode"
)

func TestDoMap(t *testing.T) {
	wc := func(filename, contents string) []KeyValue {
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		words := strings.FieldsFunc(contents, ff)
		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}
	// randReader leaves the buffer untouched (all zeros), so the random suffix
	// produced by generateUniqueIntermediateFilename is deterministic: 32 zeros.
	randReader := func(b []byte) (n int, err error) { return 0, nil }
	const zeroSuffix = "00000000000000000000000000000000"

	testCases := map[string]struct {
		// map input file
		filename      string
		contents      string
		mapTid        int
		nReduce       int
		expectedFiles map[string]string // filename -> contents
	}{
		"1": {
			filename: "test1.in",
			contents: "count the words in this file by word so count the words",
			mapTid:   0,
			nReduce:  5,
			expectedFiles: map[string]string{
				"mr-0-0-" + zeroSuffix: `{"file":["1"]}`,
				"mr-0-1-" + zeroSuffix: `{"in":["1"],"so":["1"]}`,
				"mr-0-2-" + zeroSuffix: `{"the":["1","1"],"this":["1"]}`,
				"mr-0-3-" + zeroSuffix: `{"words":["1","1"]}`,
				"mr-0-4-" + zeroSuffix: `{"by":["1"],"count":["1","1"],"word":["1"]}`,
			},
		},
	}

	for tcName, tc := range testCases {
		t.Run(tcName, func(t *testing.T) {
			ifs, err := doMap(wc, tc.filename, tc.contents, tc.mapTid, tc.nReduce, randReader)
			defer func() {
				for _, fn := range ifs {
					os.Remove(fn)
				}
			}()
			if err != nil {
				t.Fatalf("%v failed: %v", tcName, err.Error())
			}
			t.Logf("%v created files: %v", tcName, ifs)

			if len(ifs) != tc.nReduce {
				t.Fatalf("expected %v intermediate files, got %v: %v", tc.nReduce, len(ifs), ifs)
			}

			// every returned file should exist, be expected, and have matching contents
			seen := map[string]bool{}
			for _, fn := range ifs {
				expected, ok := tc.expectedFiles[fn]
				if !ok {
					t.Errorf("unexpected intermediate file %q", fn)
					continue
				}
				seen[fn] = true

				got, err := os.ReadFile(fn)
				if err != nil {
					t.Errorf("could not read intermediate file %q: %v", fn, err)
					continue
				}
				if string(got) != expected {
					t.Errorf("contents mismatch for %q:\n  got:  %s\n  want: %s", fn, string(got), expected)
				}
			}

			// make sure no expected file is missing
			for fn := range tc.expectedFiles {
				if !seen[fn] {
					t.Errorf("expected intermediate file %q was not created", fn)
				}
			}
		})
	}
}
