package wheel

import (
	"testing"
	"time"
)

func big(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

func TestGeometryValidation(t *testing.T) {
	good := []LevelSpec{
		{Name: "s", Len: 60, Span: time.Second},
		{Name: "m", Len: 60, Span: time.Minute},
		{Name: "h", Len: 24, Span: time.Hour},
		{Name: "d", Len: 31, Span: 24 * time.Hour},
	}
	if _, err := NewGeometry(0, good); err != nil {
		t.Fatalf("good geometry rejected: %v", err)
	}

	bad := map[string][]LevelSpec{
		"empty":      nil,
		"zero slots": {{Name: "s", Len: 0, Span: time.Second}},
		"zero span":  {{Name: "s", Len: 60, Span: 0}},
	}
	bad["mismatched coherence"] = []LevelSpec{
		{Name: "s", Len: 60, Span: time.Second},
		{Name: "m", Len: 45, Span: time.Minute},
		{Name: "h", Len: 24, Span: time.Hour},
	}
	for name, specs := range bad {
		if _, err := NewGeometry(0, specs); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}

	// A minute level of 90 slots needs an hour level of 90 minutes, not 60.
	if _, err := NewGeometry(0, []LevelSpec{
		{Name: "s", Len: 60, Span: time.Second},
		{Name: "m", Len: 90, Span: time.Minute},
		{Name: "h", Len: 24, Span: time.Hour},
	}); err == nil {
		t.Fatal("90-minute revolution under a 60-minute level must be rejected")
	}
	if _, err := NewGeometry(0, []LevelSpec{
		{Name: "s", Len: 60, Span: time.Second},
		{Name: "m", Len: 90, Span: time.Minute},
		{Name: "h", Len: 24, Span: 90 * time.Minute},
	}); err != nil {
		t.Fatalf("valid 90-minute geometry rejected: %v", err)
	}
}

func TestLevelForDueBoundaries(t *testing.T) {
	g, err := NewGeometry(0, []LevelSpec{
		{Name: "s", Len: 60, Span: time.Second},
		{Name: "m", Len: 60, Span: time.Minute},
		{Name: "h", Len: 24, Span: time.Hour},
		{Name: "d", Len: 31, Span: 24 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := int64(0)
	cases := []struct {
		due  time.Duration
		want int
	}{
		{0, 0},
		{time.Second, 0},
		{59 * time.Second, 0},
		{60 * time.Second, 1},
		{59*time.Minute + 59*time.Second, 1},
		{60 * time.Minute, 2},
		{23*time.Hour + 59*time.Minute, 2},
		{24 * time.Hour, 3},
		{30*24*time.Hour + 23*time.Hour, 3},
	}
	for _, c := range cases {
		got, ok := g.LevelForDue(now, int64(c.due))
		if !ok || got != c.want {
			t.Fatalf("due %s: level %d ok=%v, want %d", c.due, got, ok, c.want)
		}
	}
	// Exactly one horizon ahead is outside the wheel.
	if _, ok := g.LevelForDue(now, int64(31*24*time.Hour)); ok {
		t.Fatal("a due one full horizon ahead must not fit")
	}
	if _, ok := g.LevelForDue(now, int64(31*24*time.Hour)-1); !ok {
		t.Fatal("the last nanosecond inside the horizon must fit")
	}
}

func TestSlotStartFloor(t *testing.T) {
	g, _ := NewGeometry(0, []LevelSpec{
		{Name: "s", Len: 60, Span: time.Second},
		{Name: "m", Len: 60, Span: time.Minute},
	})
	minute := g.Levels()[1]
	for _, c := range []struct {
		due  time.Duration
		want time.Duration
	}{
		{0, 0},
		{time.Nanosecond, 0},
		{59 * time.Second, 0},
		{60 * time.Second, 60 * time.Second},
		{119 * time.Second, 60 * time.Second},
		{120 * time.Second, 120 * time.Second},
	} {
		if got := minute.SlotStart(0, int64(c.due)); got != int64(c.want) {
			t.Fatalf("due %s: slot start %s, want %s", c.due, time.Duration(got), c.want)
		}
	}
}
