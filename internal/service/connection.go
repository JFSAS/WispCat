package service

import (
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/account"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

type connectionAttempt struct {
	ID                 uint64 `json:"attempt_id"`
	StartedAt          int64  `json:"started_at"`
	Source             string `json:"source"`
	Stage              string `json:"stage"`
	DiskRead           string `json:"disk_read"`
	TicketMatchesCache *bool  `json:"ticket_matches_cache,omitempty"`
	AuthReason         string `json:"auth_reason,omitempty"`
	NextStep           string `json:"next_step"`
}

func (s *Service) beginConnectionLocked(source, stage string) {
	s.connection = connectionAttempt{ID: s.connection.ID + 1, StartedAt: time.Now().Unix(), Source: source, Stage: stage, DiskRead: "not_attempted"}
}

func (s *Service) connectionPhase(stage string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connection.Stage = stage
	if stage == "authenticate" {
		if s.connection.Source == "official_disk" {
			s.detail = "已读取官方磁盘登录材料，正在请求服务器鉴权"
		} else {
			s.detail = "正在使用缓存票据请求服务器鉴权"
		}
		s.connection.NextStep = "等待本次鉴权结果，无需重复操作。"
	}
}

// Called with s.mu held. Diagnostics describe observations, not an inferred
// account ban, credential lifetime, or raw server error text.
func (s *Service) connectionFailureLocked(err error) {
	app := strings.TrimSuffix(filepath.Base(s.source.App), ".app")
	s.connection.NextStep = "查看失败原因；可先离线查看已保存的消息。"
	if s.data != nil && s.data.LoginSource == "qr" && s.sessionState == "invalid" && s.connection.Source != "official_disk" {
		s.detail = "本项目扫码建立的登录状态已失效；缓存保留，需要手机重新扫码授权"
		s.connection.NextStep = "点击重新扫码登录申请二维码；也可以明确选择从磁盘导入登录，不会自动切换登录方式。"
		var auth *protocol.AuthenticationError
		if errors.As(err, &auth) {
			s.connection.AuthReason = auth.Reason
		}
		return
	}
	var scope *protocol.Error
	if errors.As(err, &scope) && scope.Category == "account_scope_changed" {
		s.detail = "所选微信登录的账号与当前缓存不同；原有缓存已保留，本次未开始消息收发。"
		s.connection.NextStep = "请切换到该账号对应的微信实例；当前实例的原有缓存已保留。同一微信实例更换登录账号时仍需单独处理账号缓存。"
		return
	}
	if errors.Is(err, account.ErrDiskPlatform) {
		s.category = "official_disk_import_requires_macos"
		s.detail = "当前平台不支持官方微信磁盘导入。"
		s.connection.NextStep = "请扫码登录，或迁移项目已有缓存后恢复。"
		return
	}
	var disk *account.ImportError
	if errors.As(err, &disk) {
		s.connection.Stage = disk.Stage
		s.connection.DiskRead = "failed"
		labels := map[string]string{
			"verify_installation": "安装版本校验未通过",
			"locate_files":        "未能确定登录文件位置",
			"read_config":         "配置文件读取或完整性检查未通过",
			"decode_credentials":  "配置中没有可用的账号、设备或登录票据",
			"read_key_info":       "未能找到或读取对应账号的key_info.db",
			"validate_key_info":   "登录数据库的完整性检查未通过",
			"validate_snapshot":   "读取期间磁盘登录配置发生变化",
			"crypto_profile":      "该账号使用了暂未支持的登录加密类型",
		}
		s.detail = "官方磁盘导入未完成：" + labels[disk.Stage] + "。本次尚未请求服务器鉴权。"
		s.connection.NextStep = "确认所选实例是" + app + "；完成该实例的登录，等登录文件保存后再从磁盘导入登录。"
		if disk.Stage == "verify_installation" {
			s.connection.NextStep = "检查所选微信的安装版本；当前只支持已核实的4.1.13（269602）。"
		}
		if disk.Stage == "crypto_profile" {
			s.connection.NextStep = "需要补齐该账号使用的登录类型；重复登录或再次导入未必能解决。"
		}
		if errors.Is(err, account.ErrDiskAmbiguous) {
			s.connection.NextStep = "确认所选实例的数据目录；当前有多个磁盘配置来源，重新登录不会自动解决来源不明确的问题。"
		}
		return
	}
	if s.sessionState == "invalid" {
		switch {
		case s.connection.Stage == "cache_validation":
			s.detail = "此前缓存已被标记为不可用；本次尚未读取官方磁盘，也未重新鉴权。"
			s.connection.NextStep = "选择重新扫码登录，或从磁盘导入登录，读取" + app + "的官方磁盘登录材料。"
		case s.connection.Source == "official_disk" && s.connection.DiskRead == "succeeded":
			s.detail = "已从" + app + "的官方磁盘读取登录材料，但鉴权未建立可用的独立会话。"
			s.detail += "磁盘读取成功不代表服务器仍认可其中的登录票据。"
			if s.connection.TicketMatchesCache != nil {
				if *s.connection.TicketMatchesCache {
					s.detail += "读取到的磁盘票据与原缓存相同。"
				} else {
					s.detail += "读取到的磁盘票据与原缓存不同，仍未通过验证。"
				}
			}
			s.connection.NextStep = "先检查" + app + "能否正常收发；若官方也提示登录错误，重新登录后再从磁盘导入登录。若官方正常在线，请保留此状态排查独立鉴权，不必连续重试。"
		default:
			s.detail = "使用缓存票据自动恢复未建立可用会话；本次没有重新导入官方磁盘材料。"
			s.connection.NextStep = "选择重新扫码登录，或从磁盘导入登录，从" + app + "的官方磁盘重新读取登录材料。"
		}
	} else if s.connection.Stage == "authenticate" {
		s.detail = "鉴权请求或响应处理未完成，尚不能确认登录材料是否有效。"
		if s.connection.DiskRead == "succeeded" {
			s.detail = "已读取官方磁盘登录材料；" + s.detail
		}
		s.connection.NextStep = "检查网络与当前错误类别；若官方微信正常收发，请保留诊断排查独立鉴权。"
	}
	if errors.Is(err, account.ErrVersion) {
		s.connection.NextStep = "检查所选微信的安装版本；当前只支持已核实的4.1.13（269602）。"
	}
	var auth *protocol.AuthenticationError
	if errors.As(err, &auth) {
		s.connection.AuthReason = auth.Reason
		labels := map[string]string{
			"response_format_mismatch":  "鉴权响应格式与当前协议预期不一致",
			"missing_session_material":  "鉴权响应没有包含可用会话材料",
			"response_account_mismatch": "鉴权响应的账号与请求账号不一致",
		}
		if reason := labels[auth.Reason]; reason != "" {
			s.detail += "原因：" + reason + "。"
		}
	}
}
