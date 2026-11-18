// Command server runs the delayed-message service with the dashboard.
//
// Flags:
//   -addr    listen address (default :8080)
//   -data    operation-log path (default ./data/delaymsg.log)
//   -virtual use a virtual clock instead of the wall clock
//   -speed   virtual clock speed multiplier (e.g. 3600 = 1 hour per second)
//   -demo N  preload N demo messages with a festival-eve-like skewed spread
//   -tick    scheduler tick interval in ms (default 200)
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"kitefestival/delaymsg/internal/clock"
	"kitefestival/delaymsg/internal/recur"
	"kitefestival/delaymsg/internal/scheduler"
	"kitefestival/delaymsg/internal/web"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	data := flag.String("data", "./data/delaymsg.log", "operation log path")
	useVirtual := flag.Bool("virtual", false, "use a virtual clock")
	speed := flag.Float64("speed", 60, "virtual clock speed multiplier")
	demo := flag.Int("demo", 0, "preload N demo messages")
	tickMs := flag.Int("tick", 200, "scheduler tick interval (ms)")
	flag.Parse()

	var clk clock.Clock
	if *useVirtual {
		vc := clock.NewVirtual(time.Now().UnixMilli())
		clk = vc
		go func() {
			last := time.Now()
			for range time.Tick(50 * time.Millisecond) {
				now := time.Now()
				vc.Advance(int64(now.Sub(last).Seconds() * *speed * 1000))
				last = now
			}
		}()
		log.Printf("虚拟时钟已启用，速度 ×%.0f", *speed)
	} else {
		clk = clock.Real{}
	}

	sched, err := scheduler.New(clk, *data, scheduler.Config{}, nil)
	if err != nil {
		log.Fatalf("启动失败: %v", err)
	}
	defer sched.Close()

	if *demo > 0 {
		loadDemo(sched, clk.Now(), *demo)
		log.Printf("已生成 %d 条演示消息", *demo)
	}

	go func() {
		for range time.Tick(time.Duration(*tickMs) * time.Millisecond) {
			sched.Tick()
		}
	}()

	srv := web.New(sched)
	log.Printf("看板地址: http://localhost%s/", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}

// loadDemo creates a skewed message spread resembling the kite festival:
// a huge spike on the festival eve, clusters around session starts, a long
// uniform tail, boundary-exact deadlines and a few recurrence rules.
func loadDemo(s *scheduler.Scheduler, now int64, n int) {
	rng := rand.New(rand.NewSource(7))
	eve := now - now%(24*3600*1000) + 20*3600*1000 // today 20:00
	for i := 0; i < n; i++ {
		var deadline int64
		switch p := rng.Float64(); {
		case p < 0.45: // festival-eve spike: 20:00 ± 4 min
			deadline = eve + int64(rng.NormFloat64()*4*60*1000)
		case p < 0.65: // next 10 minutes
			deadline = now + rng.Int63n(10*60*1000)
		case p < 0.85: // uniform over 7 days
			deadline = now + rng.Int63n(7*24*3600*1000)
		case p < 0.95: // boundary-exact: whole minutes/hours/days out
			unit := []int64{60 * 1000, 3600 * 1000, 24 * 3600 * 1000}[rng.Intn(3)]
			deadline = now + unit*(1+rng.Int63n(200))
		default: // past-due: immediate delivery
			deadline = now - rng.Int63n(5000)
		}
		payload := fmt.Sprintf("入场提醒 #%d", i)
		if i%2000 == 0 {
			// every flying day, 30 min before gates open
			s.Add(payload+"（周期）", 0, &recur.Rule{
				Daily: true, Time: "09:00", Offset: "-30m",
			})
			continue
		}
		s.Add(payload, deadline, nil)
	}
}
