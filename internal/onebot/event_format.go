package onebot

import (
	"sort"
	"strings"

	"github.com/JFSAS/wispcat/pkg/bot"
)

func escapeCQ(value string, parameter bool) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "[", "&#91;")
	value = strings.ReplaceAll(value, "]", "&#93;")
	if parameter {
		value = strings.ReplaceAll(value, ",", "&#44;")
	}
	return value
}
func renderCQ(segments []bot.Segment) string {
	var text strings.Builder
	for _, segment := range segments {
		if segment.Type == "text" {
			text.WriteString(escapeCQ(segment.Data["text"], false))
			continue
		}
		text.WriteString("[CQ:")
		text.WriteString(segment.Type)
		keys := make([]string, 0, len(segment.Data))
		for key := range segment.Data {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			text.WriteByte(',')
			text.WriteString(key)
			text.WriteByte('=')
			text.WriteString(escapeCQ(segment.Data[key], true))
		}
		text.WriteByte(']')
	}
	return text.String()
}

func (s *Server) finish(name, kind string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.statuses[name]
	status.Name, status.Kind = name, kind
	if err != nil {
		status.State = "failed"
		if status.Error == "" {
			status.Error = "connection_worker_failed"
		}
	} else {
		status.State = "stopped"
	}
	s.statuses[name] = status
}
