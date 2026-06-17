package source

import (
	"context"
	"os"
	"testing"

	"github.com/jack-work/schedg/internal/schema"
)

func TestDoltDescriptionLoads(t *testing.T) {
	dataDir := os.Getenv("SCHEDG_TEST_DOLT_DIR")
	if dataDir == "" {
		t.Skip("set SCHEDG_TEST_DOLT_DIR to a dolt data-dir with descriptions")
	}

	sc := schema.Default("dolt")
	t.Logf("DescCol=%q", sc.DescCol)

	src, err := Open("dolt", Config{Path: dataDir, Schema: sc})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	rows, err := src.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range rows {
		desc := r.Fields["description"]
		label := r.Fields["label"]
		t.Logf("#%s desc_len=%d label=%s", r.ID, len(desc), label)
		if len(desc) == 0 {
			t.Errorf("#%s: expected description but got empty", r.ID)
		}
	}
}
