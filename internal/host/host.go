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
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/service"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

type accountRuntime struct {
	config   Account
	channel  *service.Service
	adapter  *onebot.Server
	listener net.Listener
	http     *http.Server
}
type Host struct {
	config   Config
	lock     *os.File
	accounts []*accountRuntime
	listener net.Listener
	admin    *http.Server
	token    string
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	started  bool
	closed   bool
}

func New(c Config) (*Host, error) {
	if err := c.Validate(); err != nil {
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
	h := &Host{config: c, lock: lock}
	h.token, _ = token(c.TokenEnv)
	cleanup := func(err error) (*Host, error) {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		return nil, err
	}
	catalog, err := instances.Open(c.StateDir, c.Accounts[0].Instance)
	if err != nil {
		return cleanup(err)
	}
	for _, a := range c.Accounts {
		p, e := catalog.Ensure(a.Instance)
		if e != nil {
			return cleanup(e)
		}
		channel := service.New(p.Directory, time.Second, p.App)
		state, e := onebot.OpenState(filepath.Join(p.Directory, "onebot"))
		if e != nil {
			return cleanup(e)
		}
		secret, _ := token(a.TokenEnv)
		adapter := onebot.NewServer(onebot.New(service.BotAdapter{Service: channel}, state), secret, "")
		h.accounts = append(h.accounts, &accountRuntime{config: a, channel: channel, adapter: adapter})
	}
	return h, nil
}
func (h *Host) Start(parent context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started || h.closed {
		return errors.New("runtime_already_started_or_closed")
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
	h.admin = &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16384}
	h.wg.Add(1)
	go func() { defer h.wg.Done(); _ = h.admin.Serve(listener) }()
	for _, a := range h.accounts {
		a.channel.EnableNativeMedia()
		a.channel.Start(ctx, a.config.Mode)
		h.wg.Add(1)
		go func(a *accountRuntime) { defer h.wg.Done(); a.channel.Wait() }(a)
		a.http = &http.Server{Handler: a.adapter, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16384}
		h.wg.Add(1)
		go func(a *accountRuntime) { defer h.wg.Done(); _ = a.http.Serve(a.listener) }(a)
		for _, p := range a.config.HTTPPost {
			secret, _ := token(p.SecretEnv)
			h.wg.Add(1)
			go func(a *accountRuntime, p Post, secret string) {
				defer h.wg.Done()
				_ = a.adapter.RunPost(ctx, onebot.PostConfig{Name: p.Name, URL: p.URL, Secret: secret})
			}(a, p, secret)
		}
		for _, r := range a.config.Reverse {
			secret, _ := token(r.TokenEnv)
			h.wg.Add(1)
			go func(a *accountRuntime, r Reverse, secret string) {
				defer h.wg.Done()
				_ = a.adapter.RunReverse(ctx, onebot.ReverseConfig{Name: r.Name, URL: r.URL, Token: secret, Role: r.Role})
			}(a, r, secret)
		}
	}
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
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	started := h.started
	cancel := h.cancel
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var failures []error
	if started {
		if err := h.admin.Shutdown(ctx); err != nil {
			failures = append(failures, err)
		}
		for _, a := range h.accounts {
			if err := a.adapter.Close(ctx); err != nil {
				failures = append(failures, err)
			}
			if err := a.http.Shutdown(ctx); err != nil {
				failures = append(failures, err)
			}
		}
	}
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
		if h.lock != nil {
			_ = syscall.Flock(int(h.lock.Fd()), syscall.LOCK_UN)
			_ = h.lock.Close()
		}
	case <-ctx.Done():
		failures = append(failures, ctx.Err())
	}
	return errors.Join(failures...)
}
func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
			result = append(result, map[string]any{"name": a.config.Name, "status": a.channel.Status(), "connections": a.adapter.Statuses(), "onebot_address": a.listener.Addr().String()})
		}
		reply(200, result)
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
		case "actions":
			reply(200, a.adapter.Runtime.Actions())
		case "deliveries":
			reply(200, a.channel.Deliveries())
		default:
			reply(404, map[string]string{"error": "not_found"})
		}
		return
	}
	if r.Method != "POST" {
		reply(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	defer r.Body.Close()
	var body struct{}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(&body) != nil || dec.Decode(new(any)) != io.EOF {
		reply(400, map[string]string{"error": "expected_empty_json_object"})
		return
	}
	var err error
	switch action {
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
async function refresh(){try{const rows=await call('/accounts');out.replaceChildren();for(const a of rows){const card=document.createElement('article'),title=document.createElement('h2'),pre=document.createElement('pre');title.textContent=a.name;pre.textContent=JSON.stringify(a,null,2);card.append(title,pre);for(const [name,action] of [['恢复缓存连接','online'],['离线','offline'],['申请二维码','login/start'],['取消扫码','login/cancel']]){const b=document.createElement('button');b.textContent=name;b.onclick=async()=>{try{await call('/accounts/'+encodeURIComponent(a.name)+'/'+action,'POST');await refresh()}catch(e){alert(e.message)}};card.append(b)}const b=document.createElement('button');b.textContent='显示二维码';b.onclick=async()=>{try{const st=await call('/accounts/'+encodeURIComponent(a.name)+'/login');if(!st.image_ready)throw Error('二维码尚未就绪，请稍后刷新');const r=await fetch('/accounts/'+encodeURIComponent(a.name)+'/login/image?id='+encodeURIComponent(st.id),{headers:{Authorization:'Bearer '+token.value}});if(!r.ok)throw Error('二维码读取失败');const img=document.createElement('img'),url=URL.createObjectURL(await r.blob());img.width=240;img.onload=()=>URL.revokeObjectURL(url);img.src=url;card.append(img)}catch(e){alert(e.message)}};card.append(b);out.append(card)}}catch(e){out.textContent=e.message}}
document.querySelector('#refresh').onclick=refresh;
</script></html>`
