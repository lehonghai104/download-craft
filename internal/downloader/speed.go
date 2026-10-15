package downloader

import "time"

const speedWindow = 5 * time.Second

// SpeedMeter estimates transfer speed over the last few seconds. The zero
// value is ready to use; it is not safe for concurrent use.
type SpeedMeter struct {
	samples []speedSample
}

type speedSample struct {
	at    time.Time
	bytes int64
}

// Update records that total bytes have been transferred as of now and
// returns the recent speed in bytes per second.
func (m *SpeedMeter) Update(now time.Time, total int64) float64 {
	m.samples = append(m.samples, speedSample{now, total})
	// Keep the newest sample that is at least a window old as the baseline.
	for len(m.samples) > 2 && now.Sub(m.samples[1].at) >= speedWindow {
		m.samples = m.samples[1:]
	}
	first := m.samples[0]
	elapsed := now.Sub(first.at).Seconds()
	if elapsed <= 0 || total < first.bytes {
		return 0
	}
	return float64(total-first.bytes) / elapsed
}
