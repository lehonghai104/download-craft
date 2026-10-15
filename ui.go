package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"download-craft/internal/webui"
)

func runUI(args []string) int {
	flags := flag.NewFlagSet("download-craft ui", flag.ContinueOnError)
	addr := flags.String("addr", "127.0.0.1:7878", "`address` to serve the UI on")
	dir := flags.String("d", defaultDownloadDir(), "`directory` to save downloads into")
	conns := flags.Int("n", 8, "default number of connections per download")
	listFile := flags.String("list", defaultListFile(), "`file` that remembers the download list between runs")
	flags.Usage = func() {
		fmt.Fprint(flags.Output(), "Usage: download-craft ui [options]\n\nOpens a download manager in your browser.\n\nOptions:\n")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	mgr, err := webui.NewManager(*dir, max(*conns, 1), *listFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	ip := net.ParseIP(host)
	loopback := ip != nil && ip.IsLoopback()
	if !loopback {
		fmt.Fprintln(os.Stderr, "warning: the UI is reachable from other machines, and anyone who can reach it can download files onto this one")
		host = "localhost"
	}
	srv := &http.Server{Handler: mgr.Handler(loopback), ReadHeaderTimeout: 10 * time.Second}

	absDir, _ := filepath.Abs(*dir)
	fmt.Printf("Download Craft is running at http://%s\n", net.JoinHostPort(host, port))
	fmt.Printf("Saving downloads to %s\n", absDir)
	fmt.Println("Press Ctrl+C to quit; running downloads are paused and can continue next time.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	fmt.Println("\nPausing downloads…")
	mgr.Shutdown()
	return 0
}

func defaultDownloadDir() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, "Downloads")
	}
	return "."
}

func defaultListFile() string {
	if dir, err := os.UserConfigDir(); err == nil {
		return filepath.Join(dir, "download-craft", "downloads.json")
	}
	return ""
}
