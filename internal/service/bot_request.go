package service

import "github.com/JFSAS/wispcat/pkg/bot"

// DeliveryByRequest reads the same account-scoped identity used by Submit.
// It neither validates/reloads a payload nor schedules or retries native work.
func (a BotAdapter) DeliveryByRequest(account, key string) (bot.Delivery, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mediaScopeLocked() || account != s.data.Binding {
		return bot.Delivery{}, bot.ErrScope
	}
	if key == "" || len(key) > 160 {
		return bot.Delivery{}, bot.ErrNotFound
	}
	key = "bot/" + key
	if batch, ok := s.data.DeliveryBatches[key]; ok {
		if len(batch.IDs) == 0 {
			return bot.Delivery{}, bot.ErrStorage
		}
		for _, id := range batch.IDs {
			if s.data.Jobs[id] == nil {
				return bot.Delivery{}, bot.ErrStorage
			}
		}
		return a.batchLocked(batch.IDs)
	}
	if a.Archive != nil {
		row, err := a.Archive.batch(account, key)
		if err != nil {
			return bot.Delivery{}, err
		}
		if row != nil {
			if len(row.IDs) == 0 {
				return bot.Delivery{}, bot.ErrStorage
			}
			return a.archivedDeliveryLocked(row, row.IDs[0])
		}
	}
	return bot.Delivery{}, bot.ErrNotFound
}
