package http

/*
 * Copyright (c) 2025 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/PlakarKorp/kloset/locate"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cached"
	"github.com/dustin/go-humanize"
)

//go:embed tmpl/*
var tmpl embed.FS

type Attributes struct {
	Title string
	Items map[string]string
}

func ExecuteHTTP(ctx *appcontext.AppContext, repo *repository.Repository, mountpoint string, locateOptions *locate.LocateOptions, chrootfs fs.FS, cert string, key string) (int, error) {
	u, err := url.Parse(mountpoint)
	if err != nil {
		return 1, err
	}
	addr := u.Host

	var handler http.Handler
	if chrootfs == nil {
		mux := http.NewServeMux()
		mux.HandleFunc("/{$}", func(w http.ResponseWriter, r *http.Request) {
			ListTopOptions(w, r)
		})
		mux.HandleFunc("/snapshot/{$}", func(w http.ResponseWriter, r *http.Request) {
			ListSnapshots(ctx, repo, locateOptions, "", "", w, r)
		})
		mux.HandleFunc("/snapshot/{snapshotid}/{$}", func(w http.ResponseWriter, r *http.Request) {
			snapshotid := r.PathValue("snapshotid")
			ShowSnapshot(ctx, repo, snapshotid, w, r)
		})
		mux.HandleFunc("/tag/{tag}/{$}", func(w http.ResponseWriter, r *http.Request) {
			tag := r.PathValue("tag")
			ListSnapshots(ctx, repo, locateOptions, "", tag, w, r)
		})
		mux.HandleFunc("/tag/{$}", func(w http.ResponseWriter, r *http.Request) {
			ListTags(ctx, repo, locateOptions, w, r)
		})
		mux.HandleFunc("/origin/{origin}/{$}", func(w http.ResponseWriter, r *http.Request) {
			origin := r.PathValue("origin")
			ListSnapshots(ctx, repo, locateOptions, origin, "", w, r)
		})
		mux.HandleFunc("/origin/{$}", func(w http.ResponseWriter, r *http.Request) {
			ListOrigins(ctx, repo, locateOptions, w, r)
		})

		handler = mux
	} else {
		handler = http.FileServer(http.FS(chrootfs))
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: handler,
		// Optional: bind request contexts to app ctx
		BaseContext: func(_ net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		ctx.GetLogger().Info("HTTP serving at %s", u)
		var err error
		if u.Scheme == "http" {
			err = srv.ListenAndServe()
		} else {
			err = srv.ListenAndServeTLS(cert, key)
		}
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	// Wait for either ctx cancellation (Ctrl-C in your app) or server error.
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-errCh // wait for ListenAndServe to return
		return 0, nil
	case err := <-errCh:
		if err != nil {
			return 1, err
		}
		return 0, nil
	}
}

func ShowSnapshot(ctx *appcontext.AppContext, repo *repository.Repository, snapshotid string, w http.ResponseWriter, r *http.Request) {
	snap, path, _ := locate.OpenSnapshotByPath(repo, snapshotid)
	defer snap.Close()

	snapFS, _ := snap.Filesystem()
	subFS, _ := fs.Sub(snapFS, path[1:])
	handler := http.FileServer(http.FS(subFS))
	prefix := "/snapshot/" + snapshotid + "/"
	http.StripPrefix(prefix, handler).ServeHTTP(w, r)
}

func ListOrigins(ctx *appcontext.AppContext, repo *repository.Repository, locateOptions *locate.LocateOptions, w http.ResponseWriter, r *http.Request) {
	t := "tmpl/attributelist.html"
	res, err := tmpl.ReadFile(t)
	if err != nil {
		http.Error(w, "couldn't read template", http.StatusInternalServerError)
	}

	snapshotIDs, _, err := GetSnapshots(ctx, repo, locateOptions)
	if err != nil {
		http.Error(w, "failed to list snapshots", http.StatusInternalServerError)
		return
	}

	origins := make(map[string]string)
	for _, snapID := range snapshotIDs {
		snap, err := snapshot.Load(repo, snapID)
		if err != nil {
			continue
		}
		for _, s := range snap.Header.Sources {
			origins[s.Importer.Origin] = s.Importer.Origin
		}

		snap.Close()
	}

	attributes := Attributes{
		Title: "Origins",
		Items: origins,
	}

	tmpl, err := template.New("origins").Parse(string(res))
	if err != nil {
		http.Error(w, "couldn't parse template", http.StatusInternalServerError)
	}
	err = tmpl.Execute(w, attributes)
	if err != nil {
		http.Error(w, "couldn't process template", http.StatusInternalServerError)
	}

}

func GetSnapshots(ctx *appcontext.AppContext, repo *repository.Repository, locateOptions *locate.LocateOptions) ([]objects.MAC, map[objects.MAC]locate.Reason, error) {
	_, err := cached.RebuildStateFromStore(ctx, repo.Configuration().RepositoryID, ctx.StoreConfig, false)
	if err != nil {
		return nil, nil, err
	}

	return locate.Match(repo, locateOptions)
}

func ListTags(ctx *appcontext.AppContext, repo *repository.Repository, locateOptions *locate.LocateOptions, w http.ResponseWriter, r *http.Request) {
	t := "tmpl/attributelist.html"
	res, err := tmpl.ReadFile(t)
	if err != nil {
		http.Error(w, "couldn't read template", http.StatusInternalServerError)
	}
	snapshotIDs, _, err := GetSnapshots(ctx, repo, locateOptions)
	if err != nil {
		http.Error(w, "failed to list snapshots", http.StatusInternalServerError)
		return
	}

	tags := make(map[string]string)
	for _, snapID := range snapshotIDs {
		snap, err := snapshot.Load(repo, snapID)
		if err != nil {
			continue
		}
		for _, tag := range snap.Header.Tags {
			tags[tag] = tag
		}
		snap.Close()
	}

	tmpl, err := template.New("tags").Parse(string(res))
	if err != nil {
		http.Error(w, "couldn't parse template", http.StatusInternalServerError)
	}
	attributes := Attributes{
		Title: "Tags",
		Items: tags,
	}
	err = tmpl.Execute(w, attributes)
	if err != nil {
		http.Error(w, "couldn't process template", http.StatusInternalServerError)
	}

}

func ListSnapshots(ctx *appcontext.AppContext, repo *repository.Repository, locateOptions *locate.LocateOptions, origin string, tag string, w http.ResponseWriter, r *http.Request) {
	snap := "tmpl/snapshots.html"
	res, err := tmpl.ReadFile(snap)
	if err != nil {
		http.Error(w, "couldn't read template", http.StatusInternalServerError)
	}
	type Snapshot struct {
		ID        string
		ShortID   string
		Origin    string
		Time      string
		TimeStamp time.Time
		Tags      string
	}

	snapshotIDs, _, err := GetSnapshots(ctx, repo, locateOptions)
	if err != nil {
		http.Error(w, "failed to list snapshots", http.StatusInternalServerError)
		return
	}

	snapshots := make([]Snapshot, 0, len(snapshotIDs))
	for _, snapID := range snapshotIDs {
		snap, err := snapshot.Load(repo, snapID)
		if err != nil {
			continue
		}
		origins := make([]string, 0, len(snap.Header.Sources))
		for _, s := range snap.Header.Sources {
			origins = append(origins, s.Importer.Origin)
		}
		if origin != "" && !slices.Contains(origins, origin) {
			continue
		}
		if tag != "" && !slices.Contains(snap.Header.Tags, tag) {
			continue
		}

		snapshot := Snapshot{
			ID:        fmt.Sprintf("%x", snap.Header.Identifier),
			ShortID:   fmt.Sprintf("%x", snap.Header.Identifier[0:4]),
			Origin:    strings.Join(origins, ", "),
			Time:      humanize.Time(snap.Header.Timestamp),
			TimeStamp: snap.Header.Timestamp,
			Tags:      strings.Join(snap.Header.Tags, ", "),
		}
		snapshots = append(snapshots, snapshot)
		snap.Close()
	}

	slices.SortFunc(snapshots, func(a, b Snapshot) int {
		return b.TimeStamp.Compare(a.TimeStamp)
	})

	tmpl, err := template.New("snapshots").Parse(string(res))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	err = tmpl.Execute(w, snapshots)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func ListTopOptions(w http.ResponseWriter, r *http.Request) {
	top := "tmpl/topoptions.html"
	res, err := tmpl.ReadFile(top)
	if err != nil {
		http.Error(w, "couldn't read template", http.StatusInternalServerError)
	}
	w.Write(res)
}
