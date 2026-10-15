// Command download-craft downloads a file over several parallel connections
// and joins the parts into the final file, like Internet Download Manager.
//
//	download-craft [options] <url>     download from the command line
//	download-craft ui [options]        open the download manager in a browser
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"download-craft/internal/downloader"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "ui" {
		os.Exit(runUI(args[1:]))
	}
	os.Exit(runCLI(args))
}

// headerFlag collects repeated -H "Name: value" flags.
type headerFlag http.Header

func (h headerFlag) String() string { return "" }

func (h headerFlag) Set(v string) error {
	name, value, ok := strings.Cut(v, ":")
	if !ok || strings.TrimSpace(name) == "" {
		return fmt.Errorf(`want "Name: value", got %q`, v)
	}
	http.Header(h).Add(strings.TrimSpace(name), strings.TrimSpace(value))
	return nil
}

// parseInterspersed parses flags that may appear before or after the
// positional arguments, e.g. "download-craft <url> -n 16".
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return positional, nil
		}
		positional = append(positional, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func runCLI(args []string) int {
	flags := flag.NewFlagSet("download-craft", flag.ContinueOnError)
	conns := flags.Int("n", 8, "number of parallel connections")
	output := flags.String("o", "", "output file `name or path` (default: name from the server or URL)")
	dir := flags.String("d", "", "`directory` to save into")
	retries := flags.Int("r", 5, "failed attempts in a row, with no progress at all, before giving up")
	force := flags.Bool("f", false, "overwrite the output file if it exists")
	quiet := flags.Bool("q", false, "do not show progress")
	header := headerFlag{}
	flags.Var(header, "H", "extra request header `\"Name: value\"` (repeatable)")
	flags.Usage = func() {
		fmt.Fprint(flags.Output(), "Usage:\n  download-craft [options] <url>\n  download-craft ui [options]   (run with ui -h for its options)\n\nOptions:\n")
		flags.PrintDefaults()
	}

	urls, err := parseInterspersed(flags, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if len(urls) != 1 {
		flags.Usage()
		return 2
	}
	if *conns < 1 {
		fmt.Fprintln(os.Stderr, "error: -n must be at least 1")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	started := time.Now()
	d, err := downloader.New(ctx, downloader.Options{
		URL:         urls[0],
		Output:      *output,
		Dir:         *dir,
		Connections: *conns,
		Retries:     *retries,
		Header:      http.Header(header),
		Overwrite:   *force,
	})
	if err == nil {
		if !*quiet {
			describe(d)
		}
		stopProgress := func() {}
		if !*quiet {
			stopProgress = showProgress(os.Stderr, d)
		}
		err = d.Run(ctx)
		stopProgress()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "Interrupted.")
		} else {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
		if errors.Is(err, fs.ErrExist) {
			fmt.Fprintln(os.Stderr, "Use -f to overwrite it, or -o to choose another name.")
		}
		var inc *downloader.IncompleteError
		if errors.As(err, &inc) {
			fmt.Fprintf(os.Stderr, "Partial download kept in %s; run the same command again to resume.\n", inc.PartsDir)
		}
		if errors.Is(err, context.Canceled) {
			return 130
		}
		return 1
	}

	if !*quiet {
		elapsed := time.Since(started)
		fi, _ := os.Stat(d.Path())
		fetched := fi.Size() - d.Resumed()
		fmt.Fprintf(os.Stderr, "Saved %s (%s) in %s, average %s/s\n",
			d.Path(), formatBytes(fi.Size()), elapsed.Round(100*time.Millisecond),
			formatBytes(int64(float64(fetched)/elapsed.Seconds())))
	}
	return 0
}

func describe(d *downloader.Download) {
	parts := len(d.Stats().Parts)
	size := "unknown size"
	if d.Size() >= 0 {
		size = formatBytes(d.Size())
	}
	fmt.Fprintf(os.Stderr, "Saving to %s\n", d.Path())
	switch {
	case !d.Resumable():
		fmt.Fprintf(os.Stderr, "%s, 1 connection (the server does not support split downloads)\n", size)
	case d.Resumed() > 0:
		fmt.Fprintf(os.Stderr, "%s in %d parts, resuming from %s\n", size, parts, formatBytes(d.Resumed()))
	default:
		fmt.Fprintf(os.Stderr, "%s in %d parts\n", size, parts)
	}
}
