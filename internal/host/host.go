// Package host composes protocol channels and robot interfaces, without any
// model, provider, prompt or automatic reply dependency.
package host

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/native"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/service"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type accountRuntime struct {
	config      Account
	connections *connectionSettings
	inbound     *inboundSettings
	channel     *service.Service
	bot         service.BotAdapter
	adapter     *onebot.Server
	listener    net.Listener
}
type Host struct {
	config        Config
	lock          *os.File
	state         *onebot.State
	accounts      []*accountRuntime
	listener      net.Listener
	admin         *http.Server
	token         string
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	mu            sync.Mutex
	started       bool
	closed        bool
	requests      chan struct{}
	shutdownDone  chan struct{}
	shutdownError error
}

func New(c Config) (*Host, error) {
	var err error
	c, err = effectiveConfig(c)
	if err != nil {
		return nil, err
	}
	if err := store.Directory(c.StateDir); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(c.StateDir, "backend.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errors.New("state_root_in_use")
	}
	h := &Host{config: c, lock: lock, requests: make(chan struct{}, 16)}
	h.token, _ = token(c.TokenEnv)
	cleanup := func(err error) (*Host, error) {
		for _, a := range h.accounts {
			if a.adapter.Runtime.Streams != nil {
				_ = a.adapter.Runtime.Streams.Close()
			}
			_ = a.bot.CloseHistory()
			if a.bot.Archive != nil {
				_ = a.bot.Archive.Close()
			}
		}
		if h.state != nil {
			_ = h.state.Close()
		}
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		return nil, err
	}
	catalog, err := instances.Open(c.StateDir, c.Accounts[0].Instance)
	if err != nil {
		return cleanup(err)
	}
	profiles := map[string]instances.Profile{}
	for _, a := range c.Accounts {
		p, e := catalog.Ensure(a.Instance)
		if e != nil {
			return cleanup(e)
		}
		profiles[a.Name] = p
	}
	// Include temporarily disabled instances before allocating any new public
	// IDs, so a later re-enable cannot lose its legacy numbers to a new account.
	registered, err := catalog.List()
	if err != nil {
		return cleanup(err)
	}
	legacy := make([]string, 0, len(registered))
	for _, p := range registered {
		legacy = append(legacy, filepath.Join(p.Directory, "onebot"))
	}
	shared, e := onebot.OpenSharedState(filepath.Join(c.StateDir, "onebot"), legacy)
	if e != nil {
		return cleanup(e)
	}
	h.state = shared
	for _, a := range c.Accounts {
		p := profiles[a.Name]
		archive, e := service.OpenDeliveryArchive(p.Directory)
		if e != nil {
			return cleanup(e)
		}
		if e = archive.Recover(p.Directory); e != nil {
			_ = archive.Close()
			return cleanup(e)
		}
		if e = service.RecoverBotIntents(p.Directory, archive); e != nil {
			_ = archive.Close()
			return cleanup(e)
		}
		channel := service.New(p.Directory, time.Second, p.App)
		bot := service.BotAdapter{Service: channel, Archive: archive}
		history, e := service.OpenEventHistory(p.Directory)
		if e != nil {
			_ = archive.Close()
			return cleanup(e)
		}
		if e = bot.EnableHistory(history); e != nil {
			_ = history.Close()
			_ = archive.Close()
			return cleanup(e)
		}
		if e = bot.RecoverOutgoingMedia(); e != nil {
			_ = history.Close()
			_ = archive.Close()
			return cleanup(e)
		}
		state := shared
		secret, _ := token(a.TokenEnv)
		adapter := onebot.NewServer(onebot.New(bot, state), secret, "")
		streams, e := onebot.OpenStreams(filepath.Join(p.Directory, "stream-temp"))
		if e != nil {
			_ = history.Close()
			_ = archive.Close()
			return cleanup(e)
		}
		adapter.Runtime.Streams = streams
		settings, e := openConnectionSettings(p.Directory, c, a)
		if e != nil {
			_ = streams.Close()
			_ = history.Close()
			_ = archive.Close()
			return cleanup(e)
		}
		h.accounts = append(h.accounts, &accountRuntime{config: a, channel: channel, bot: bot, adapter: adapter, connections: settings,
			inbound: newInboundSettings(p.Directory, c, a)})
	}
	return h, nil
}
func (h *Host) Start(parent context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started || h.closed {
		return errors.New("runtime_already_started_or_closed")
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	for _, a := range h.accounts {
		if a.config.Mode == "online" && !native.Supported() {
			return errors.New("native_transport_unavailable")
		}
	}
	initial := map[*accountRuntime][]onebot.NetworkSpec{}
	for _, a := range h.accounts {
		plan, err := a.connections.specs(a.connections.saved)
		if err != nil {
			return err
		}
		initial[a] = plan
	}
	// Bind all listeners before starting any account. A bad topology cannot
	// leave a partial set of live sessions behind.
	listener, err := net.Listen("tcp", h.config.Listen)
	if err != nil {
		return err
	}
	h.listener = listener
	for _, a := range h.accounts {
		a.listener, err = net.Listen("tcp", a.config.Listen)
		if err != nil {
			_ = listener.Close()
			for _, v := range h.accounts {
				if v.listener != nil {
					_ = v.listener.Close()
				}
			}
			return err
		}
		a.adapter.Host = a.listener.Addr().String()
	}
	ctx, cancel := context.WithCancel(parent)
	h.cancel = cancel
	h.started = true
	h.admin = &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16384}
	for _, a := range h.accounts {
		a.bot.EnableNativeMedia()
		a.channel.Start(ctx, a.config.Mode)
		h.wg.Add(1)
		go func(a *accountRuntime) { defer h.wg.Done(); a.bot.RunArchive(ctx) }(a)
		h.wg.Add(1)
		go func(a *accountRuntime) { defer h.wg.Done(); a.channel.Wait() }(a)
		a.inbound.mu.Lock()
		a.inbound.manager = newInboundServer(a.adapter, ctx, a.listener, a.config.Listen, a.adapter.Token)
		a.inbound.active = a.inbound.revision
		a.inbound.mu.Unlock()
		a.connections.manager = onebot.NewNetworkManager(a.adapter, ctx)
		if err := a.connections.manager.Apply(ctx, initial[a]); err != nil {
			cancel()
			return err
		}
		a.connections.mu.Lock()
		a.connections.active = a.connections.revision
		a.connections.mu.Unlock()

	}
	h.wg.Add(1)
	go func() { defer h.wg.Done(); _ = h.admin.Serve(listener) }()
	return nil
}
func (h *Host) Address() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.listener == nil {
		return ""
	}
	return h.listener.Addr().String()
}
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	if h.shutdownDone == nil {
		h.shutdownDone = make(chan struct{})
		h.closed = true
		go h.shutdown()
	}
	done := h.shutdownDone
	h.mu.Unlock()
	select {
	case <-done:
		h.mu.Lock()
		err := h.shutdownError
		h.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *Host) shutdown() {
	h.mu.Lock()
	started, cancel := h.started, h.cancel
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
	defer stop()
	var failures []error
	if started {
		if err := h.admin.Shutdown(ctx); err != nil {
			failures = append(failures, err)
		}
		if h.listener != nil {
			_ = h.listener.Close()
		}
		for _, a := range h.accounts {
			if a.inbound.manager != nil {
				if err := a.inbound.manager.close(ctx); err != nil {
					failures = append(failures, err)
				}
			}
			if a.connections.manager != nil {
				if err := a.connections.manager.Close(ctx); err != nil {
					failures = append(failures, err)
				}
			}
			if err := a.adapter.Close(ctx); err != nil {
				failures = append(failures, err)
			}
			if a.listener != nil {
				_ = a.listener.Close()
			}
		}
	} else {
		if h.listener != nil {
			_ = h.listener.Close()
		}
		for _, a := range h.accounts {
			if a.listener != nil {
				_ = a.listener.Close()
			}
		}
	}
	// A caller deadline does not abandon ownership cleanup or permit another
	// process to start while old protocol work is still in flight.
	h.wg.Wait()
	for _, a := range h.accounts {
		// A request that ignored its cancellation still owns the account's
		// storage. Caller/HTTP deadlines cannot release it underneath that work.
		if a.inbound.manager != nil {
			_ = a.inbound.manager.close(context.Background())
		}
		_ = a.adapter.Close(context.Background())
		if a.adapter.Runtime.Streams != nil {
			if err := a.adapter.Runtime.Streams.Close(); err != nil {
				failures = append(failures, err)
			}
		}
		if err := a.bot.CloseHistory(); err != nil {
			failures = append(failures, err)
		}
		if a.bot.Archive != nil {
			if err := a.bot.Archive.Close(); err != nil {
				failures = append(failures, err)
			}
		}
	}
	if h.state != nil {
		if err := h.state.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	if h.lock != nil {
		_ = syscall.Flock(int(h.lock.Fd()), syscall.LOCK_UN)
		_ = h.lock.Close()
	}
	h.mu.Lock()
	h.shutdownError = errors.Join(failures...)
	close(h.shutdownDone)
	h.mu.Unlock()
}
func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case h.requests <- struct{}{}:
		defer func() { <-h.requests }()
	default:
		http.Error(w, "management_concurrency_limit", 503)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Host != h.Address() {
		http.Error(w, "host_not_allowed", 403)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host {
		http.Error(w, "origin_not_allowed", 403)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, managementPage)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+h.token)) != 1 {
		http.Error(w, "authentication_required", 401)
		return
	}
	reply := func(code int, value any) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(value)
	}
	if r.URL.Path == "/accounts" && r.Method == "GET" {
		result := []any{}
		for _, a := range h.accounts {
			address := a.listener.Addr().String()
			if a.inbound.manager != nil {
				state, _ := a.inbound.manager.snapshot("", "")
				address = state.Address
			}
			result = append(result, map[string]any{"name": a.config.Name, "status": a.channel.Status(), "connections": a.adapter.Statuses(), "onebot_address": address})
		}
		reply(200, result)
		return
	}
	if r.URL.Path == "/storage" && r.Method == "GET" {
		stats, err := h.state.Stats()
		if err != nil {
			reply(503, map[string]string{"error": "state_storage_unavailable"})
			return
		}
		reply(200, stats)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "accounts" {
		reply(404, map[string]string{"error": "not_found"})
		return
	}
	var a *accountRuntime
	for _, v := range h.accounts {
		if v.config.Name == parts[1] {
			a = v
			break
		}
	}
	if a == nil {
		reply(404, map[string]string{"error": "account_not_configured"})
		return
	}
	action := strings.Join(parts[2:], "/")
	if r.Method == "GET" {
		switch action {
		case "status":
			reply(200, a.channel.Status())
		case "login":
			reply(200, a.channel.LoginStatus())
		case "login/image":
			data, err := a.channel.LoginImage(r.URL.Query().Get("id"))
			if err != nil {
				reply(404, map[string]string{"error": "login_image_unavailable"})
				return
			}
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(data)
		case "connections":
			reply(200, a.connections.view())
		case "listener":
			reply(200, a.inbound.view())
		case "actions":
			reply(200, a.adapter.Runtime.Actions())
		case "deliveries":
			reply(200, a.channel.Deliveries())
		case "archive":
			status, err := a.bot.ArchiveStatus()
			if err != nil {
				reply(503, map[string]string{"error": "delivery_archive_unavailable"})
				return
			}
			reply(200, status)
		case "history":
			status, err := a.bot.HistoryStatus()
			if err != nil {
				reply(503, map[string]string{"error": "event_history_unavailable"})
				return
			}
			reply(200, status)
		case "streams":
			status, err := a.adapter.Runtime.Streams.Snapshot("")
			if err != nil {
				reply(503, map[string]string{"error": "stream_storage_unavailable"})
				return
			}
			reply(200, status)
		case "media-cache":
			status, err := a.bot.IncomingMediaStatus()
			if err != nil {
				reply(503, map[string]string{"error": "incoming_media_storage_unavailable"})
				return
			}
			reply(200, status)
		case "outgoing-media":
			status, err := a.bot.OutgoingMediaStatus()
			if err != nil {
				reply(503, map[string]string{"error": "outgoing_media_storage_unavailable"})
				return
			}
			reply(200, status)
		default:
			reply(404, map[string]string{"error": "not_found"})
		}
		return
	}
	if r.Method != "POST" {
		reply(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	limit := int64(8192)
	if action == "connections" {
		limit = 64 << 10
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	defer r.Body.Close()
	if action == "listener" {
		var input struct {
			Revision string      `json:"revision"`
			Plan     inboundPlan `json:"plan"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF {
			reply(400, map[string]string{"error": "invalid_listener_config"})
			return
		}
		if err := a.inbound.save(input.Revision, input.Plan); err != nil {
			reply(409, map[string]string{"error": "listener_save_failed"})
			return
		}
		reply(200, a.inbound.view())
		return
	}
	if action == "connections" {
		var input struct {
			Revision string         `json:"revision"`
			Plan     connectionPlan `json:"plan"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF {
			reply(400, map[string]string{"error": "invalid_connections_config"})
			return
		}
		if err := a.connections.save(input.Revision, input.Plan); err != nil {
			reply(409, map[string]string{"error": "connections_save_failed"})
			return
		}
		reply(200, a.connections.view())
		return
	}
	var body struct{}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
		reply(400, map[string]string{"error": "expected_empty_json_object"})
		return
	}
	if (action == "online" || action == "login/start" || action == "login/disk") && !native.Supported() {
		reply(409, map[string]string{"error": "native_transport_unavailable"})
		return
	}
	var err error
	switch action {
	case "outgoing-media/compact":
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		result, e := a.bot.ReclaimOutgoingMedia(ctx)
		if e != nil {
			reply(409, map[string]string{"error": "outgoing_media_reclamation_pending"})
			return
		}
		reply(200, result)
		return
	case "media-cache/compact":
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		result, e := a.bot.ReclaimIncomingMedia(ctx)
		if e != nil {
			reply(409, map[string]string{"error": "incoming_media_reclamation_pending"})
			return
		}
		reply(200, result)
		return
	case "streams/clean":
		result, e := a.adapter.Runtime.Streams.Clean("")
		if e != nil {
			reply(503, map[string]string{"error": "stream_storage_unavailable"})
			return
		}
		reply(200, result)
		return
	case "archive/compact":
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		report, e := a.bot.ArchiveCompleted(ctx)
		if e != nil {
			reply(409, map[string]string{"error": "delivery_archive_pending"})
			return
		}
		reply(200, report)
		return
	case "connections/apply":
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		err = a.connections.apply(ctx)
	case "listener/apply":
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		err = a.inbound.apply(ctx)
	case "online":
		err = a.channel.SetMode("online", false)
	case "offline":
		err = a.channel.SetMode("offline", false)
	case "login/start":
		_, err = a.channel.QueueLogin()
	case "login/cancel":
		a.channel.CancelLogin()
	case "login/disk":
		err = a.channel.QueueDiskLogin()
	default:
		reply(404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		reply(409, map[string]string{"error": "account_operation_failed"})
		return
	}
	reply(202, map[string]string{"status": "queued"})
}

const managementPage = `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>WeChat Runtime</title><style>body{max-width:960px;margin:40px auto;font:16px system-ui;background:#f6f7f9;color:#17212c}input,button{padding:10px;margin:5px}pre{background:white;padding:20px;white-space:pre-wrap}article{background:white;padding:20px;margin:15px 0;border-radius:8px}</style><h1>WeChat Runtime</h1><p>独立微信通道 · OneBot v11。Bot 和模型在外部进程运行。</p><input id="token" type="password" placeholder="管理令牌" autocomplete="off"><button id="refresh">刷新账号</button><main id="accounts"></main><script>
const token=document.querySelector('#token'),out=document.querySelector('#accounts');
async function call(path,method='GET'){const r=await fetch(path,{method,headers:{Authorization:'Bearer '+token.value,...(method==='POST'?{'Content-Type':'application/json'}:{})},...(method==='POST'?{body:'{}'}:{})});if(!r.ok)throw Error('HTTP '+r.status);return r.json()}
async function loadListener(card,name){
 const title=document.createElement('h3');title.textContent='OneBot 入站监听';
 const hint=document.createElement('p');hint.textContent='保存后单独应用。换端口或令牌会断开旧入站请求和 WebSocket，微信会话继续运行。';
 const editor=document.createElement('textarea');editor.rows=4;editor.style.width='95%';editor.setAttribute('aria-label',name+' 入站监听配置');
 const info=document.createElement('pre');let revision='';
 const path='/accounts/'+encodeURIComponent(name)+'/listener';
 async function load(){const state=await call(path);revision=state.revision;editor.value=JSON.stringify(state.saved,null,2);info.textContent=JSON.stringify({applied:state.applied,active_revision:state.active_revision,listener:state.listener,error:state.error},null,2)}
 const save=document.createElement('button');save.textContent='保存入站监听';
 save.onclick=async()=>{try{const plan=JSON.parse(editor.value);const response=await fetch(path,{method:'POST',headers:{Authorization:'Bearer '+token.value,'Content-Type':'application/json'},body:JSON.stringify({revision,plan})});if(!response.ok)throw Error('保存失败：HTTP '+response.status);await load()}catch(e){alert(e.message)}};
 const apply=document.createElement('button');apply.textContent='应用入站监听';apply.onclick=async()=>{try{await call(path+'/apply','POST');await load();await refresh()}catch(e){alert(e.message);await load()}};
 card.append(title,hint,editor,save,apply,info);await load()
}
async function refresh(){try{const rows=await call('/accounts');out.replaceChildren();for(const a of rows){const card=document.createElement('article'),title=document.createElement('h2'),pre=document.createElement('pre');title.textContent=a.name;pre.textContent=JSON.stringify(a,null,2);card.append(title,pre);for(const [name,action] of [['恢复缓存连接','online'],['离线','offline'],['申请二维码','login/start'],['取消扫码','login/cancel']]){const b=document.createElement('button');b.textContent=name;b.onclick=async()=>{try{await call('/accounts/'+encodeURIComponent(a.name)+'/'+action,'POST');await refresh()}catch(e){alert(e.message)}};card.append(b)}const b=document.createElement('button');b.textContent='显示二维码';b.onclick=async()=>{try{const st=await call('/accounts/'+encodeURIComponent(a.name)+'/login');if(!st.image_ready)throw Error('二维码尚未就绪，请稍后刷新');const r=await fetch('/accounts/'+encodeURIComponent(a.name)+'/login/image?id='+encodeURIComponent(st.id),{headers:{Authorization:'Bearer '+token.value}});if(!r.ok)throw Error('二维码读取失败');const img=document.createElement('img'),url=URL.createObjectURL(await r.blob());img.width=240;img.onload=()=>URL.revokeObjectURL(url);img.src=url;card.append(img)}catch(e){alert(e.message)}};card.append(b);const archive=document.createElement('pre');archive.hidden=true;for(const [label,path,method] of [['投递归档状态','archive','GET'],['归档已完成投递','archive/compact','POST'],['临时传输状态','streams','GET'],['清理传输临时文件','streams/clean','POST'],['入站媒体缓存','media-cache','GET'],['回收入站缓存','media-cache/compact','POST'],['发送媒体状态','outgoing-media','GET'],['回收已确认发送副本','outgoing-media/compact','POST']]){const button=document.createElement('button');button.textContent=label;button.onclick=async()=>{try{const result=await call('/accounts/'+encodeURIComponent(a.name)+'/'+path,method);archive.textContent=JSON.stringify(result,null,2);archive.hidden=false}catch(e){alert(e.message)}};card.append(button)}card.append(archive);const editor=document.createElement('textarea');editor.rows=12;editor.style.width='95%';const info=document.createElement('p');let revision='';async function loadConnections(){const state=await call('/accounts/'+encodeURIComponent(a.name)+'/connections');revision=state.revision;editor.value=JSON.stringify(state.saved,null,2);info.textContent=state.applied?'连接配置已应用':'已保存连接配置尚未应用'}const save=document.createElement('button');save.textContent='保存连接配置';save.onclick=async()=>{try{const plan=JSON.parse(editor.value);const response=await fetch('/accounts/'+encodeURIComponent(a.name)+'/connections',{method:'POST',headers:{Authorization:'Bearer '+token.value,'Content-Type':'application/json'},body:JSON.stringify({revision,plan})});if(!response.ok)throw Error('保存失败：HTTP '+response.status);await loadConnections()}catch(e){alert(e.message)}};const apply=document.createElement('button');apply.textContent='应用已保存连接';apply.onclick=async()=>{try{await call('/accounts/'+encodeURIComponent(a.name)+'/connections/apply','POST');await loadConnections()}catch(e){alert(e.message)}};card.append(info,editor,save,apply);out.append(card);await loadConnections();await loadListener(card,a.name)}}catch(e){out.textContent=e.message}}
document.querySelector('#refresh').onclick=refresh;
</script></html>`
