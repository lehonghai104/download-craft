# download-craft

A download accelerator in the style of Internet Download Manager. It splits a
file into parts, downloads the parts in parallel over separate connections,
and joins them into the final file. Use it from the command line or from a
small download manager in your browser.

Single binary, Go standard library only.

## Build

```sh
go build -o download-craft .
```

## Command line

```sh
download-craft [options] <url>
```

```
$ download-craft -n 8 https://example.com/ubuntu.iso
Saving to ubuntu.iso
5.7 GiB in 8 parts
 45.2% [██▒░░███▒░░██▒░░█▒░░] 2.6 GiB/5.7 GiB  48.3 MiB/s  ETA 01:05
```

The bar shows where in the file the downloaded bytes are, so you can watch
each part fill in from its own starting point.

| Option | Default | Meaning |
|---|---|---|
| `-n` | 8 | parallel connections (parts) |
| `-o` | from server | output file name or path |
| `-d` | current dir | directory to save into |
| `-H "Name: value"` | | extra request header, repeatable (cookies, auth, referer) |
| `-r` | 5 | retries before giving up (see "Retries") |
| `-f` | | overwrite an existing file |
| `-q` | | no progress output |

Flags can go before or after the URL.

**Resume:** press Ctrl+C and the parts stay on disk in `<file>.parts/`. Run
the same command again to continue where it stopped. If the link has expired,
run it again with a fresh link and the same `-o`. It resumes as long as the
server reports the same file (same size, ETag and Last-Modified).

## Browser UI

```sh
download-craft ui            # then open http://127.0.0.1:7878
```

Paste a link, choose the number of connections, and press **Download**. Each
download shows a segmented bar, one segment per part, with its speed and time
left. The buttons are:

- **Pause**: stops the download and keeps the parts.
- **Continue**: resumes a paused or failed download.
- **Stop**: cancels the download and deletes the partial data.
- **Remove**: clears a finished entry from the list. The downloaded file is kept.

Adding the same link twice saves `name (1).ext` instead of overwriting.

The download list is saved in `~/.config/download-craft/downloads.json`.
Ctrl+C pauses running downloads, and they come back paused the next time the
UI starts.

| Option | Default | Meaning |
|---|---|---|
| `-addr` | `127.0.0.1:7878` | where to serve the UI |
| `-d` | `~/Downloads` | directory to save into |
| `-n` | 8 | default connections per download |
| `-list` | `~/.config/download-craft/downloads.json` | where the list is kept (`""` to not keep it) |
| `-localhost` | | require a localhost address even when `-addr` is not one (see "Docker") |

By default the UI only listens on localhost. Anyone who can reach it can make
this machine download files, so it rejects requests from other web sites and
requests that don't name a localhost address. If you bind `-addr` to a LAN
address, everyone on that network can use it.

## Docker

```sh
docker compose up -d       # then open http://localhost:7878
```

This builds the image and runs the browser UI. Downloads are saved in
`~/Downloads`, and the download list is kept in a Docker volume. The container
runs as user and group 1000. If `id -u` or `id -g` prints something else, set
`user:` in `compose.yaml`.

Without Compose:

```sh
docker build -t download-craft .
docker run -d --name download-craft -p 127.0.0.1:7878:7878 \
  --user "$(id -u):$(id -g)" \
  -v ~/Downloads:/downloads -v download-craft-config:/config \
  download-craft
```

Pass a URL instead to use the command line. The file is saved in the current
directory:

```sh
docker run --rm -it --user "$(id -u):$(id -g)" -v "$PWD":/downloads \
  download-craft -n 16 https://example.com/ubuntu.iso
```

The UI inside the container listens on all interfaces, but it is started with
`-localhost`, so it still rejects requests that don't name a localhost
address. Publish the port on `127.0.0.1` as shown. If you publish it on every
interface, the rest of your network can still reach the UI by sending
`Host: localhost`.

## How it works

1. **Probe:** request `Range: bytes=0-0`. A `206` reply proves the server
   supports ranges and gives the total size. The name comes from
   `Content-Disposition`, or from the URL path.
2. **Split:** divide the size into `-n` parts, none smaller than 1 MiB. Save
   the layout to `<file>.parts/state.json` so a later run can resume.
3. **Download:** each part gets its own HTTP/1.1 connection and its own file,
   `<file>.parts/part-NNN`. HTTP/2 is deliberately not used: it would put
   every part on a single TCP connection.
4. **Join:** check each part's size, then concatenate them in order. The first
   part is renamed rather than copied, and each later part is deleted after it
   is appended, so joining needs little extra disk space. Then remove the
   parts directory.

Servers without range support are downloaded over a single connection.
Pausing those downloads cannot keep partial data.

### Retries

- **Dropped connection:** the part continues from the byte where it stopped.
- **Stalled connection:** a connection with no data for 30 s is aborted and
  retried.
- **Permanent errors:** 403, 404 and other client errors fail immediately.
- **Connection limits:** many servers cap connections per client and turn the
  extra ones away. A part therefore only gives up after `-r` consecutive
  failures during which the whole download made no progress. While other parts
  are moving, the rejected parts keep retrying, with back-off.

## Development

```sh
go test -race ./...
```

The tests run against local servers that drop connections, stall, ignore
ranges, limit concurrent connections, or change the file between runs.
