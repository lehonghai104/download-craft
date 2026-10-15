package downloader

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
)

// remoteFile is what the server tells us about the file before downloading.
type remoteFile struct {
	size         int64 // -1 when the server does not report it
	rangeable    bool  // the server honours Range requests, so the file can be split
	name         string
	etag         string
	lastModified string
}

// probe asks for the first byte of the file. A 206 reply proves the server
// supports ranges and carries the total size in Content-Range; this is more
// reliable than HEAD, which many servers answer differently from GET.
func probe(ctx context.Context, client *http.Client, rawURL string, header http.Header) (*remoteFile, error) {
	req, err := newRequest(ctx, rawURL, header)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	rf := &remoteFile{
		size:         -1,
		name:         fileName(resp),
		etag:         resp.Header.Get("ETag"),
		lastModified: resp.Header.Get("Last-Modified"),
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		_, _, total, err := parseContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return nil, err
		}
		rf.size = total
		rf.rangeable = total > 0
	case http.StatusOK:
		// The server ignored Range and started sending the whole file.
		rf.size = resp.ContentLength
	case http.StatusRequestedRangeNotSatisfiable:
		// Byte 0 does not exist, which only happens for an empty file.
		if _, _, total, err := parseContentRange(resp.Header.Get("Content-Range")); err != nil || total != 0 {
			return nil, fmt.Errorf("server responded %s", resp.Status)
		}
		rf.size = 0
	default:
		return nil, fmt.Errorf("server responded %s", resp.Status)
	}
	return rf, nil
}

func newRequest(ctx context.Context, rawURL string, header http.Header) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header[k] = append([]string(nil), v...)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", userAgent)
	}
	return req, nil
}

// parseContentRange parses "bytes 0-99/1234", "bytes 0-99/*" and "bytes */1234".
// Unknown values are returned as -1.
func parseContentRange(v string) (start, end, total int64, err error) {
	bad := fmt.Errorf("invalid Content-Range %q", v)
	rest, ok := strings.CutPrefix(v, "bytes ")
	if !ok {
		return 0, 0, 0, bad
	}
	rng, size, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, 0, 0, bad
	}
	start, end, total = -1, -1, -1
	if size != "*" {
		if total, err = strconv.ParseInt(size, 10, 64); err != nil || total < 0 {
			return 0, 0, 0, bad
		}
	}
	if rng != "*" {
		s, e, ok := strings.Cut(rng, "-")
		if !ok {
			return 0, 0, 0, bad
		}
		if start, err = strconv.ParseInt(s, 10, 64); err != nil {
			return 0, 0, 0, bad
		}
		if end, err = strconv.ParseInt(e, 10, 64); err != nil || end < start {
			return 0, 0, 0, bad
		}
	}
	return start, end, total, nil
}

// fileName picks a local name from Content-Disposition, falling back to the
// last segment of the (post-redirect) URL path.
func fileName(resp *http.Response) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if name := sanitizeName(params["filename"]); name != "" {
				return name
			}
		}
	}
	if name := sanitizeName(path.Base(resp.Request.URL.Path)); name != "" {
		return name
	}
	return "download"
}

// sanitizeName strips any directory components so a hostile server cannot
// make us write outside the download directory.
func sanitizeName(name string) string {
	name = strings.TrimSpace(path.Base(strings.ReplaceAll(name, `\`, "/")))
	switch name {
	case ".", "..", "/":
		return ""
	}
	return name
}
