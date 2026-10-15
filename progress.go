package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"download-craft/internal/downloader"
)

const (
	barWidth        = 20
	refreshInterval = 200 * time.Millisecond
	logInterval     = 5 * time.Second // between lines when not on a terminal
)

// showProgress redraws a one-line progress display on w until stop is
// called. On a terminal the line is redrawn in place; otherwise a line is
// printed every few seconds so logs stay readable.
func showProgress(w io.Writer, d *downloader.Download) (stop func()) {
	tty := isTerminal(w)
	var meter downloader.SpeedMeter
	meter.Update(time.Now(), d.Stats().Downloaded)
	var lastLog time.Time

	render := func(final bool) {
		now := time.Now()
		s := d.Stats()
		line := progressLine(s, meter.Update(now, s.Downloaded), final)
		switch {
		case tty:
			fmt.Fprintf(w, "\r%s\033[K", line)
			if final {
				fmt.Fprintln(w)
			}
		case final || now.Sub(lastLog) >= logInterval:
			fmt.Fprintln(w, line)
			lastLog = now
		}
	}

	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				render(false)
			case <-quit:
				render(true)
				return
			}
		}
	}()
	return func() {
		close(quit)
		<-done
	}
}

// progressLine fits in 79 columns, e.g.
// " 45.2% [██▒░░███▒░░██▒░░█▒░░] 452.0 MiB/1.0 GiB  12.3 MiB/s  ETA 00:45"
func progressLine(s downloader.Stats, speed float64, final bool) string {
	if s.Size <= 0 {
		return fmt.Sprintf("%s  %s/s", formatBytes(s.Downloaded), formatBytes(int64(speed)))
	}
	status := "ETA --:--"
	switch {
	case final && s.Downloaded == s.Size:
		status = "done"
	case s.Joining:
		status = "joining…"
	case speed > 0:
		status = "ETA " + formatDuration(time.Duration(float64(s.Size-s.Downloaded)/speed*float64(time.Second)))
	}
	return fmt.Sprintf("%5.1f%% [%s] %s/%s  %s/s  %s",
		100*float64(s.Downloaded)/float64(s.Size), segmentBar(s, barWidth),
		formatBytes(s.Downloaded), formatBytes(s.Size), formatBytes(int64(speed)), status)
}

// segmentBar draws where in the file the downloaded bytes are, so each part
// can be seen filling in from its own starting point.
func segmentBar(s downloader.Stats, width int) string {
	cellSize := float64(s.Size) / float64(width)
	filled := make([]float64, width) // fraction of each cell downloaded
	for _, p := range s.Parts {
		from, to := float64(p.Start), float64(p.Start+p.Done)
		for c := int(from / cellSize); c < width && float64(c)*cellSize < to; c++ {
			lo := max(from, float64(c)*cellSize)
			hi := min(to, float64(c+1)*cellSize)
			if hi > lo {
				filled[c] += (hi - lo) / cellSize
			}
		}
	}
	var b strings.Builder
	for _, f := range filled {
		switch {
		case f >= 0.999:
			b.WriteRune('█')
		case f > 0:
			b.WriteRune('▒')
		default:
			b.WriteRune('░')
		}
	}
	return b.String()
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	s := int64(d.Round(time.Second) / time.Second)
	switch {
	case s >= 100*3600:
		return "--:--"
	case s >= 3600:
		return fmt.Sprintf("%d:%02d:%02d", s/3600, s/60%60, s%60)
	default:
		return fmt.Sprintf("%02d:%02d", s/60, s%60)
	}
}
