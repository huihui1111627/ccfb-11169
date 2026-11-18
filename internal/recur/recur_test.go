package recur

import (
	"testing"
	"time"
)

var loc = time.FixedZone("CST", 8*3600)

func ms(s string) int64 {
	t, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
	if err != nil {
		panic(err)
	}
	return t.UnixMilli()
}

func TestDailyWithOffset(t *testing.T) {
	c, err := Compile(&Rule{Daily: true, Time: "09:00", Offset: "-30m"}, loc)
	if err != nil {
		t.Fatal(err)
	}
	// 每个放飞日开场前半小时：09:00 开场 → 08:30 提醒
	next, ok := c.NextAfter(ms("2026-04-19 08:00"))
	if !ok || next != ms("2026-04-19 08:30") {
		t.Fatalf("got %v %v", time.UnixMilli(next), ok)
	}
	// 当天的已过 → 次日
	next, ok = c.NextAfter(ms("2026-04-19 08:30"))
	if !ok || next != ms("2026-04-20 08:30") {
		t.Fatalf("got %v %v", time.UnixMilli(next), ok)
	}
}

func TestWeekdays(t *testing.T) {
	// 2026-04-19 是周日
	c, err := Compile(&Rule{Weekdays: []int{6, 0}, Time: "10:00"}, loc)
	if err != nil {
		t.Fatal(err)
	}
	next, _ := c.NextAfter(ms("2026-04-19 10:00")) // 周日 10:00 刚过 → 下周六
	if next != ms("2026-04-25 10:00") {
		t.Fatalf("got %v", time.UnixMilli(next))
	}
}

func TestDatesAndUntil(t *testing.T) {
	c, err := Compile(&Rule{Dates: []string{"2026-04-19", "2026-04-20"}, Time: "09:00"}, loc)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.NextAfter(ms("2026-04-20 09:00")); ok {
		t.Fatal("两个放飞日都过后不应再有触发")
	}
	c2, err := Compile(&Rule{Daily: true, Time: "09:00", Until: "2026-04-21"}, loc)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c2.NextAfter(ms("2026-04-21 09:00")); ok {
		t.Fatal("until 之后不应再有触发")
	}
}

// 规则写错必须在添加时就报出来。
func TestInvalidRules(t *testing.T) {
	bad := []*Rule{
		{Time: "09:00"},                                  // 没有日期选择器
		{Daily: true, Weekdays: []int{1}, Time: "09:00"}, // 选择器冲突
		{Daily: true, Time: "9点"},                       // 时间格式错
		{Daily: true, Time: "25:00"},                     // 时间越界
		{Daily: true, Time: "09:00", Offset: "半小时"},    // 偏移格式错
		{Weekdays: []int{7}, Time: "09:00"},              // 星期越界
		{Dates: []string{"4月19日"}, Time: "09:00"},       // 日期格式错
		{Daily: true, Time: "09:00", Until: "2026/04/19"},
	}
	for i, r := range bad {
		if _, err := Compile(r, loc); err == nil {
			t.Fatalf("bad rule #%d should be rejected: %+v", i, r)
		}
	}
}
