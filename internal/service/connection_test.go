package service

import (
	"errors"
	"strings"
	"testing"

	"github.com/JFSAS/wispcat/internal/account"
	"github.com/JFSAS/wispcat/internal/protocol"
)

func TestDiskReadSuccessAndAuthenticationFailureAreDistinct(t *testing.T) {
	for _, same := range []bool{true, false} {
		s := recoveryFixture(t)
		s.connection = connectionAttempt{Source: "official_disk", Stage: "authenticate", DiskRead: "succeeded", TicketMatchesCache: &same}
		s.block(&protocol.Error{Category: "authentication_rejected"})
		if s.state != "disconnected" || s.connection.DiskRead != "succeeded" || !strings.Contains(s.detail, "已从") || !strings.Contains(s.detail, "未建立可用") {
			t.Fatal("successful disk import was described as a read failure")
		}
		if !strings.Contains(s.connection.NextStep, "能否正常收发") || !strings.Contains(s.connection.NextStep, "不必连续重试") {
			t.Fatal("authentication failure lacks a conditional next step")
		}
		word := "不同"
		if same {
			word = "相同"
		}
		if !strings.Contains(s.detail, word) {
			t.Fatal("ticket comparison was described inaccurately")
		}
	}
}

func TestDiskImportFailureReportsItsStepWithoutSensitiveErrorText(t *testing.T) {
	s := recoveryFixture(t)
	s.connection = connectionAttempt{Source: "official_disk", Stage: "read_disk", DiskRead: "reading"}
	s.block(&account.ImportError{Stage: "read_key_info", Err: errors.New("SYNTHETIC_PRIVATE_DATA")})
	if s.connection.DiskRead != "failed" || s.connection.Stage != "read_key_info" || !strings.Contains(s.detail, "本次尚未请求服务器鉴权") {
		t.Fatal("disk failure was confused with server authentication")
	}
	if strings.Contains(s.detail+s.connection.NextStep+s.category, "SYNTHETIC_PRIVATE_DATA") || s.connection.NextStep == "" {
		t.Fatal("disk diagnostics exposed error contents or omitted the next step")
	}
}

func TestKnownInvalidCacheDoesNotClaimANewServerRejection(t *testing.T) {
	s := recoveryFixture(t)
	s.connection = connectionAttempt{Source: "cache", Stage: "cache_validation", DiskRead: "not_attempted"}
	s.block(&protocol.Error{Category: "authentication_rejected"})
	if !strings.Contains(s.detail, "本次尚未读取官方磁盘") || !strings.Contains(s.connection.NextStep, "从磁盘导入登录") {
		t.Fatal("known invalid cache was presented as a new disk/auth attempt")
	}
}

func TestAutomaticRecoveryExplainsThatDiskWasNotRead(t *testing.T) {
	s := recoveryFixture(t)
	r := sessionRecovery{}
	s.recoverFailure(&protocol.Error{Category: "session_expired", Code: -13}, &r)
	s.connectionPhase("authenticate")
	s.recoverFailure(&protocol.Error{Category: "authentication_rejected"}, &r)
	if s.connection.Source != "cache" || s.connection.DiskRead != "not_attempted" || !strings.Contains(s.detail, "没有重新导入官方磁盘") {
		t.Fatal("automatic cache refresh was presented as a disk import")
	}
}

func TestAuthenticationReasonDoesNotPretendToBeAServerErrorCode(t *testing.T) {
	s := recoveryFixture(t)
	s.connection = connectionAttempt{Source: "official_disk", Stage: "authenticate", DiskRead: "succeeded"}
	s.block(&protocol.AuthenticationError{Reason: "missing_session_material"})
	if s.category != "authentication_rejected" || s.connection.AuthReason != "missing_session_material" || !strings.Contains(s.detail, "没有包含可用会话材料") {
		t.Fatal("specific authentication response failure was lost")
	}
}
