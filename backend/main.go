package main

import (
	"encoding/json"
	"fmt"
	"html"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
)

type serverConfig struct {
	Name          string `json:"display_name,omitempty"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Directory     string `json:"directory"`
	MaxAge        int64  `json:"maxAge"`
	CacheControl  bool   `json:"cacheControl"`
	LastModified  bool   `json:"lastModified"`
	Etag          bool   `json:"etag"`
	Index         string `json:"index"`
	Redirect      bool   `json:"redirect"`
	Dotfiles      string `json:"dotfiles"`
	AcceptRanges  bool   `json:"acceptRanges"`
	BackgroundRun bool   `json:"backgroundRun"`
}
type running struct {
	srv  *http.Server
	addr string
}
type plugin struct {
	mutex   sync.Mutex
	servers map[string]running
	configs map[string]serverConfig
}

func displayName(c serverConfig) string {
	if c.Name != "" {
		return c.Name
	}
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func configMap(c serverConfig) map[string]any {
	return map[string]any{"display_name": displayName(c), "host": c.Host, "port": c.Port, "directory": c.Directory, "maxAge": c.MaxAge, "cacheControl": c.CacheControl, "lastModified": c.LastModified, "etag": c.Etag, "index": c.Index, "redirect": c.Redirect, "dotfiles": c.Dotfiles, "acceptRanges": c.AcceptRanges, "backgroundRun": c.BackgroundRun}
}

func (p *plugin) Handle(_ dbxpluginsdk.RequestContext, method string, params json.RawMessage, _ *dbxpluginsdk.Emitter) (any, *dbxpluginsdk.PluginError) {
	var values map[string]any
	if err := json.Unmarshal(params, &values); err != nil {
		return nil, dbxpluginsdk.NewError(-32602, "Invalid request parameters")
	}
	switch method {
	case "frames/close":
		id, _ := values["connectionId"].(string)
		if id == "" {
			id, _ = values["connection_id"].(string)
		}
		if id == "" {
			return nil, dbxpluginsdk.NewError(-32602, "Missing connectionId")
		}
		p.mutex.Lock()
		cfg, ok := p.configs[id]
		p.mutex.Unlock()
		if ok && !cfg.BackgroundRun {
			p.stop(id)
			p.mutex.Lock()
			delete(p.configs, id)
			p.mutex.Unlock()
		}
		return map[string]any{"success": true}, nil
	case "static-file-server/config":
		id, _ := values["connectionId"].(string)
		if id == "" {
			id, _ = values["connection_id"].(string)
		}
		// hasInput := values["directory"] != nil || values["host"] != nil || values["port"] != nil || values["config"] != nil
		// incoming := parseConfig(values)
		// if hasInput {
		// 	return map[string]any{"config": configMap(incoming)}, nil
		// }
		p.mutex.Lock()
		cfg, ok := p.configs[id]
		p.mutex.Unlock()
		if !ok {
			return nil, dbxpluginsdk.NewError(-32004, "Connection configuration not found")
		}
		return map[string]any{"config": configMap(cfg)}, nil
	case "static-file-server/start":
		id, _ := values["connectionId"].(string)
		if id == "" {
			id, _ = values["connection_id"].(string)
		}
		if id == "" {
			return nil, dbxpluginsdk.NewError(-32602, "Missing connectionId")
		}
		cfg := parseConfig(values)
		p.mutex.Lock()
		p.configs[id] = cfg
		p.mutex.Unlock()
		if err := p.start(id, cfg); err != nil {
			return nil, dbxpluginsdk.NewError(-32000, err.Error())
		}
		return map[string]any{"success": true, "address": p.address(id), "message": fmt.Sprintf("Static File Server %s:%d", cfg.Host, cfg.Port)}, nil
	case "static-file-server/stop":
		id, _ := values["connectionId"].(string)
		if id == "" {
			id, _ = values["connection_id"].(string)
		}
		if id == "" {
			return nil, dbxpluginsdk.NewError(-32602, "Missing connectionId")
		}
		p.stop(id)
		return map[string]any{"success": true}, nil
	case "connection/test":
		cfg := parseConfig(values)
		return map[string]any{"success": true, "message": "Static File Server is ready", "fieldValues": map[string]any{"display_name": displayName(cfg)}}, nil
	case "connection/connect":
		id, e := requestConnectionID(values)
		if e != nil {
			return nil, e
		}
		cfg := parseConfig(values)
		p.mutex.Lock()
		p.configs[id] = cfg
		p.mutex.Unlock()
		if cfg.BackgroundRun {
			if err := p.start(id, cfg); err != nil {
				return nil, dbxpluginsdk.NewError(-32000, err.Error())
			}
		}
		return map[string]any{"success": true, "address": p.address(id), "fieldValues": map[string]any{"display_name": displayName(cfg)}}, nil
	case "connection/disconnect", "connections/disconnect":
		id, e := requestConnectionID(values)
		if e != nil {
			return nil, e
		}
		p.mutex.Lock()
		cfg, keepRunning := p.configs[id]
		p.mutex.Unlock()
		if keepRunning && !cfg.BackgroundRun {
			p.stop(id)
			p.mutex.Lock()
			delete(p.configs, id)
			p.mutex.Unlock()
		}
		return map[string]any{"success": true}, nil
	case "connection/update":
		id, e := requestConnectionID(values)
		if e != nil {
			return nil, e
		}
		p.stop(id)
		cfg := parseConfig(values)
		p.mutex.Lock()
		p.configs[id] = cfg
		p.mutex.Unlock()
		if err := p.start(id, cfg); err != nil {
			return nil, dbxpluginsdk.NewError(-32000, err.Error())
		}
		return map[string]any{"success": true, "address": p.address(id), "fieldValues": map[string]any{"display_name": fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)}}, nil
	default:
		return nil, dbxpluginsdk.MethodNotFound(method)
	}
}
func parseConfig(v map[string]any) serverConfig {
	flattenConfig(v, v)
	c := serverConfig{Host: "127.0.0.1", Port: 3456, Directory: func() string { h, _ := os.UserHomeDir(); return h }(), MaxAge: 31536000000, CacheControl: true, LastModified: true, Etag: true, Index: "index.html", Redirect: true, Dotfiles: "allow", AcceptRanges: true}
	if x, ok := v["connection"].(map[string]any); ok {
		for k, val := range x {
			v[k] = val
		}
		if config, ok := x["config"].(map[string]any); ok {
			for k, val := range config {
				v[k] = val
			}
		}
		if config, ok := x["values"].(map[string]any); ok {
			for k, val := range config {
				v[k] = val
			}
		}
	}
	if x, ok := v["config"].(map[string]any); ok {
		for k, val := range x {
			v[k] = val
		}
	}
	if raw, ok := v["config"].(string); ok {
		var config map[string]any
		if json.Unmarshal([]byte(raw), &config) == nil {
			for k, val := range config {
				v[k] = val
			}
		}
	}
	if s, ok := v["host"].(string); ok && s != "" {
		c.Host = s
	}
	if s, ok := v["display_name"].(string); ok {
		c.Name = s
	}
	if c.Name == "" {
		if s, ok := v["name"].(string); ok {
			c.Name = s
		}
	}
	if n, ok := v["port"].(float64); ok {
		c.Port = int(n)
	}
	if s, ok := v["directory"].(string); ok && s != "" {
		c.Directory = s
	}
	if n, ok := v["maxAge"].(float64); ok {
		c.MaxAge = int64(n)
	}
	if b, ok := v["cacheControl"].(bool); ok {
		c.CacheControl = b
	}
	if b, ok := v["lastModified"].(bool); ok {
		c.LastModified = b
	}
	if b, ok := v["etag"].(bool); ok {
		c.Etag = b
	}
	if s, ok := v["index"].(string); ok && s != "" {
		c.Index = s
	}
	if b, ok := v["redirect"].(bool); ok {
		c.Redirect = b
	}
	if s, ok := v["dotfiles"].(string); ok {
		c.Dotfiles = s
	}
	if b, ok := v["acceptRanges"].(bool); ok {
		c.AcceptRanges = b
	}
	if b, ok := v["backgroundRun"].(bool); ok {
		c.BackgroundRun = b
	}
	return c
}

func flattenConfig(dst, src map[string]any) {
	for _, key := range []string{"connection", "config", "values", "settings", "external_config"} {
		if nested, ok := src[key].(map[string]any); ok {
			for k, value := range nested {
				if k != "connection" && k != "config" && k != "values" && k != "settings" && k != "external_config" {
					dst[k] = value
				}
			}
			flattenConfig(dst, nested)
		}
	}
}
func (p *plugin) start(id string, c serverConfig) error {
	root, err := filepath.Abs(c.Directory)
	if err != nil {
		return err
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := filepath.Clean("/" + r.URL.Path)
		if strings.Contains(rel, "..") {
			http.NotFound(w, r)
			return
		}
		if c.Dotfiles != "allow" {
			for _, part := range strings.Split(rel, "/") {
				if strings.HasPrefix(part, ".") {
					if c.Dotfiles == "deny" {
						http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
					} else {
						http.NotFound(w, r)
					}
					return
				}
			}
		}
		file := filepath.Join(root, rel)
		st, err := os.Stat(file)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		if st.IsDir() {
			if c.Redirect && !strings.HasSuffix(r.URL.Path, "/") {
				http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
				return
			}
			indexPath := filepath.Join(file, c.Index)
			if indexInfo, indexErr := os.Stat(indexPath); indexErr == nil && !indexInfo.IsDir() {
				file, st = indexPath, indexInfo
			} else {
				entries, readErr := os.ReadDir(file)
				if readErr != nil {
					http.Error(w, "Forbidden", http.StatusForbidden)
					return
				}
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				if r.Method == http.MethodHead {
					return
				}
				fmt.Fprintf(w, "<!doctype html><meta charset=\"utf-8\"><title>Index of %s</title><h1>Index of %s</h1><ul>", html.EscapeString(r.URL.Path), html.EscapeString(r.URL.Path))
				if r.URL.Path != "/" {
					fmt.Fprint(w, `<li><a href="../">../</a></li>`)
				}
				for _, entry := range entries {
					name := entry.Name()
					if strings.HasPrefix(name, ".") && c.Dotfiles != "allow" {
						continue
					}
					display := name
					href := url.PathEscape(name)
					if entry.IsDir() {
						display += "/"
						href += "/"
					}
					fmt.Fprintf(w, `<li><a href="%s">%s</a></li>`, href, html.EscapeString(display))
				}
				fmt.Fprint(w, "</ul>")
				return
			}
		}
		if c.CacheControl {
			w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", c.MaxAge/1000))
		}
		if c.LastModified {
			w.Header().Set("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))
		}
		if typ := mime.TypeByExtension(filepath.Ext(file)); typ != "" {
			w.Header().Set("Content-Type", typ)
		}
		http.ServeFile(w, r, file)
	})
	srv := &http.Server{Addr: netAddr(c.Host, c.Port), Handler: h}
	go srv.ListenAndServe()
	p.mutex.Lock()
	if old, ok := p.servers[id]; ok {
		old.srv.Close()
	}
	p.servers[id] = running{srv: srv, addr: srv.Addr}
	p.mutex.Unlock()
	return nil
}
func netAddr(h string, p int) string { return h + ":" + strconv.Itoa(p) }
func (p *plugin) stop(id string) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if r, ok := p.servers[id]; ok {
		r.srv.Close()
		delete(p.servers, id)
	}
}
func (p *plugin) address(id string) string {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.servers[id].addr
}
func requestConnectionID(v map[string]any) (string, *dbxpluginsdk.PluginError) {
	c, _ := v["connection"].(map[string]any)
	id, _ := c["id"].(string)
	if id == "" {
		id, _ = v["connectionId"].(string)
	}
	if id == "" {
		id, _ = v["connection_id"].(string)
	}
	if id == "" {
		return "", dbxpluginsdk.NewError(-32602, "Missing connection id")
	}
	return id, nil
}
func main() {
	m := dbxpluginsdk.Metadata{ID: "com.kingreatwill.static-file-server", Version: "0.1.0", Capabilities: []string{"connections"}}
	if err := dbxpluginsdk.NewServer(m, &plugin{servers: map[string]running{}, configs: map[string]serverConfig{}}).Serve(); err != nil {
		log.Fatal(err)
	}
}
