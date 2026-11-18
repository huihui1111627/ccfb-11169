// Package web serves the operations dashboard: layered wheel state,
// soon-to-expire messages, delivered messages, recurrence rules and stats.
package web

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"kitefestival/delaymsg/internal/recur"
	"kitefestival/delaymsg/internal/scheduler"
)

// Server exposes the dashboard and a small JSON API.
type Server struct {
	sched *scheduler.Scheduler
	mux   *http.ServeMux
}

func New(sched *scheduler.Scheduler) *Server {
	s := &Server{sched: sched, mux: http.NewServeMux()}
	s.mux.HandleFunc("/", s.handleIndex)
	s.mux.HandleFunc("/api/state", s.handleState)
	s.mux.HandleFunc("/api/add", s.handleAdd)
	s.mux.HandleFunc("/api/cancel", s.handleCancel)
	s.mux.HandleFunc("/api/resched", s.handleResched)
	s.mux.HandleFunc("/api/ack", s.handleAck)
	s.mux.HandleFunc("/api/compact", s.handleCompact)
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(page))
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.sched.Snapshot())
}

type addRequest struct {
	Payload    string      `json:"payload"`
	DelayMs    int64       `json:"delayMs"`
	DeadlineMs int64       `json:"deadlineMs"`
	Rule       *recur.Rule `json:"rule"`
}

func (s *Server) handleAdd(w http.ResponseWriter, r *http.Request) {
	var req addRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "请求体不是合法 JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	deadline := req.DeadlineMs
	if deadline == 0 && req.Rule == nil {
		deadline = time.Now().UnixMilli() + req.DelayMs
	}
	id, err := s.sched.Add(req.Payload, deadline, req.Rule)
	if err != nil {
		http.Error(w, "添加失败: "+err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]interface{}{"id": id})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "id 不合法", http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]bool{"ok": s.sched.Cancel(id)})
}

func (s *Server) handleResched(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, err1 := strconv.ParseUint(q.Get("id"), 10, 64)
	deadline, err2 := strconv.ParseInt(q.Get("deadlineMs"), 10, 64)
	if err1 != nil || err2 != nil {
		http.Error(w, "参数不合法", http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]bool{"ok": s.sched.Reschedule(id, deadline)})
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64)
	if err != nil {
		http.Error(w, "id 不合法", http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]bool{"ok": s.sched.Ack(id)})
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	s.sched.Compact()
	writeJSON(w, map[string]bool{"ok": true})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(v)
}

const page = `<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>风筝节 · 延时消息调度台</title>
<style>
  :root { --bg:#0f1420; --card:#1a2233; --line:#2a3550; --fg:#dbe4f5; --dim:#8fa0c0; --acc:#4fc3f7; --warn:#ffb74d; --ok:#81c784; }
  * { box-sizing: border-box; }
  body { margin:0; font:14px/1.5 -apple-system,"PingFang SC",sans-serif; background:var(--bg); color:var(--fg); }
  header { padding:14px 22px; border-bottom:1px solid var(--line); display:flex; align-items:baseline; gap:16px; flex-wrap:wrap; }
  header h1 { font-size:18px; margin:0; }
  header .clock { color:var(--acc); font-variant-numeric:tabular-nums; }
  main { padding:18px 22px; display:grid; gap:16px; }
  section { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
  h2 { margin:0 0 10px; font-size:15px; color:var(--acc); }
  table { width:100%; border-collapse:collapse; font-variant-numeric:tabular-nums; }
  th,td { text-align:left; padding:5px 8px; border-bottom:1px solid var(--line); white-space:nowrap; }
  th { color:var(--dim); font-weight:500; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; }
  .card { background:var(--bg); border:1px solid var(--line); border-radius:8px; padding:10px 12px; }
  .card .n { font-size:22px; font-weight:600; }
  .card .l { color:var(--dim); font-size:12px; }
  .neg { color:var(--warn); } .pos { color:var(--ok); }
  .muted { color:var(--dim); }
  form { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  input,textarea,button { background:var(--bg); color:var(--fg); border:1px solid var(--line); border-radius:6px; padding:6px 9px; font:inherit; }
  button { cursor:pointer; background:#24314d; }
  button:hover { border-color:var(--acc); }
  .err { color:#ef9a9a; }
  #addResult { font-size:13px; }
</style>
</head>
<body>
<header>
  <h1>🪁 风筝节 · 延时消息调度台</h1>
  <span class="clock" id="now">–</span>
  <span class="muted" id="refresh"></span>
</header>
<main>
  <section>
    <h2>统计</h2>
    <div class="cards" id="statCards"></div>
  </section>
  <section>
    <h2>各层时间轮（空转跳格 · 到期误差 = 投递/降级时刻 − 应到时刻，负为提前，正为延后）</h2>
    <table id="layers">
      <thead><tr><th>层</th><th>每格跨度</th><th>格子数</th><th>当前指针</th><th>非空格</th><th>格内消息</th><th>误差次数</th><th>平均误差</th><th>最大提前</th><th>最大延后</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>
  <section>
    <h2>即将到期（前 50 条）</h2>
    <table id="soon">
      <thead><tr><th>消息号</th><th>内容</th><th>到期时刻</th><th>倒计时</th><th>周期</th><th>操作</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>
  <section>
    <h2>已投递（最近 200 条，待确认可 ack）</h2>
    <table id="delivered">
      <thead><tr><th>消息号</th><th>内容</th><th>应到时刻</th><th>投递时刻</th><th>误差</th><th>立即</th><th>操作</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>
  <section>
    <h2>周期规则（前 50 条）</h2>
    <table id="rules">
      <thead><tr><th>消息号</th><th>内容</th><th>规则</th><th>下次触发</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>
  <section>
    <h2>添加消息</h2>
    <form id="addForm">
      <input name="payload" placeholder="内容，如：3 号场地放飞 10 分钟后开始" size="30" required>
      <input name="delayMs" placeholder="多少毫秒后（留空则用规则）" size="18">
      <input name="rule" placeholder='周期规则 JSON，如 {"daily":true,"time":"09:00","offset":"-30m"}' size="46">
      <button>添加</button>
      <span id="addResult"></span>
    </form>
  </section>
</main>
<script>
const fmtT = ms => ms == null ? "–" : new Date(ms).toLocaleString("zh-CN",{hour12:false}) + "." + String(ms%1000).padStart(3,"0");
const fmtDur = ms => {
  if (ms == null) return "–";
  const neg = ms < 0; ms = Math.abs(ms);
  const s = Math.floor(ms/1000);
  const u = [[86400,"天"],[3600,"时"],[60,"分"]];
  for (const [v,n] of u) if (s >= v) return (neg?"-":"") + Math.floor(s/v) + n + Math.floor(s%v/(v===86400?3600:v===3600?60:1)) + (v===86400?"时":v===3600?"分":"秒");
  return (neg?"-":"") + s + "秒";
};
const fmtErr = ms => ms == null ? "–" : '<span class="' + (ms<0?"neg":ms>0?"pos":"") + '">' + (ms<0?"提前":ms>0?"延后":"") + Math.abs(ms) + "ms</span>";

async function refresh() {
  const s = await (await fetch("/api/state")).json();
  document.getElementById("now").textContent = "当前时钟 " + fmtT(s.Now);
  const st = s.Stats;
  document.getElementById("statCards").innerHTML = [
    [st.WheelPending, "轮中待投递"], [st.Delivered, "累计投递"], [st.Unacked, "待确认"],
    [st.Cancelled, "已取消"], [st.Immediate, "过立即刻投递"], [st.Compactions, "日志压紧次数"],
    [(st.LogBytes/1024).toFixed(0)+" KB", "日志大小"], [st.LogOps, "压紧后日志条数"],
  ].map(([n,l]) => '<div class="card"><div class="n">'+n+'</div><div class="l">'+l+'</div></div>').join("");

  document.querySelector("#layers tbody").innerHTML = s.Layers.map(l => {
    const stt = l.Stats;
    const avg = stt.Count ? Math.round(stt.Sum/stt.Count) : null;
    return "<tr><td>"+l.Name+"</td><td>"+fmtDur(l.TickMs)+"</td><td>"+l.Size+"</td><td>"+fmtT(l.Current)+"</td><td>"+l.Buckets+"</td><td>"+l.Pending+"</td><td>"+stt.Count+"</td><td>"+fmtErr(avg)+"</td><td>"+fmtErr(stt.Count?stt.Min:null)+"</td><td>"+fmtErr(stt.Count?stt.Max:null)+"</td></tr>";
  }).join("");

  document.querySelector("#soon tbody").innerHTML = (s.Soon||[]).map(m =>
    "<tr><td>"+m.ID+"</td><td>"+esc(m.Payload)+"</td><td>"+fmtT(m.Deadline)+"</td><td>"+fmtDur(m.InMs)+"</td><td>"+(m.Recur?"↻":"")+"</td>"+
    '<td><button onclick="cancelMsg('+m.ID+')">取消</button> <button onclick="reschedMsg('+m.ID+')">改期</button></td></tr>').join("");

  document.querySelector("#delivered tbody").innerHTML = (s.Delivered||[]).map(d =>
    "<tr><td>"+d.ID+"</td><td>"+esc(d.Payload)+"</td><td>"+fmtT(d.Deadline)+"</td><td>"+fmtT(d.At)+"</td><td>"+fmtErr(d.ErrMs)+"</td><td>"+(d.Immediate?"是":"")+"</td>"+
    '<td><button onclick="ackMsg('+d.ID+')">确认</button></td></tr>').join("");

  document.querySelector("#rules tbody").innerHTML = (s.Rules||[]).map(r =>
    "<tr><td>"+r.ID+"</td><td>"+esc(r.Payload)+"</td><td>"+esc(r.Rule)+"</td><td>"+fmtT(r.Next)+"</td></tr>").join("");
}
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

async function cancelMsg(id){ await fetch("/api/cancel?id="+id,{method:"POST"}); refresh(); }
async function ackMsg(id){ await fetch("/api/ack?id="+id,{method:"POST"}); refresh(); }
async function reschedMsg(id){
  const v = prompt("新的到期时间（毫秒时间戳，或 +毫秒 表示相对现在）:");
  if (!v) return;
  let ms = v.trim().startsWith("+") ? Date.now() + parseInt(v.trim().slice(1)) : parseInt(v.trim());
  if (!Number.isFinite(ms)) { alert("不合法"); return; }
  await fetch("/api/resched?id="+id+"&deadlineMs="+ms,{method:"POST"}); refresh();
}
document.getElementById("addForm").onsubmit = async e => {
  e.preventDefault();
  const f = e.target, out = document.getElementById("addResult");
  const body = { payload: f.payload.value };
  if (f.delayMs.value) body.delayMs = parseInt(f.delayMs.value);
  if (f.rule.value.trim()) { try { body.rule = JSON.parse(f.rule.value); } catch(err){ out.innerHTML='<span class="err">规则 JSON 解析失败: '+err+'</span>'; return; } }
  const r = await fetch("/api/add", {method:"POST", body: JSON.stringify(body)});
  out.innerHTML = r.ok ? '<span class="pos">已添加</span>' : '<span class="err">'+esc(await r.text())+'</span>';
  if (r.ok) f.reset();
  refresh();
};
refresh(); setInterval(refresh, 2000);
</script>
</body>
</html>`
