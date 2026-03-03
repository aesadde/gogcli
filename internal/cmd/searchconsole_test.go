package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/api/option"
	"google.golang.org/api/webmasters/v3"

	"github.com/steipete/gogcli/internal/outfmt"
)

func TestParseDimensionFilter(t *testing.T) {
	filter, err := parseDimensionFilter("query:contains:shoes")
	if err != nil {
		t.Fatalf("parseDimensionFilter: %v", err)
	}

	if filter.Dimension != "query" {
		t.Fatalf("unexpected dimension: %q", filter.Dimension)
	}
	if filter.Operator != "contains" {
		t.Fatalf("unexpected operator: %q", filter.Operator)
	}
	if filter.Expression != "shoes" {
		t.Fatalf("unexpected expression: %q", filter.Expression)
	}

	if _, err := parseDimensionFilter("query:contains"); err == nil {
		t.Fatalf("expected error for invalid filter")
	}
}

func TestSearchConsoleSearchAnalyticsQueryCmd_BuildRequest(t *testing.T) {
	cmd := &SearchConsoleSearchAnalyticsQueryCmd{
		StartDate:       "2026-02-01",
		EndDate:         "2026-02-28",
		Dimensions:      "query,page",
		SearchType:      "web",
		AggregationType: "byPage",
		DataState:       "final",
		Limit:           250,
		StartRow:        10,
		Filter:          []string{"query:contains:buy shoes", "country:equals:usa"},
		FilterGroupType: "and",
	}

	req, err := cmd.buildRequest()
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}

	if req.StartDate != "2026-02-01" || req.EndDate != "2026-02-28" {
		t.Fatalf("unexpected date range: %s - %s", req.StartDate, req.EndDate)
	}
	if req.RowLimit != 250 || req.StartRow != 10 {
		t.Fatalf("unexpected pagination: limit=%d startRow=%d", req.RowLimit, req.StartRow)
	}
	if len(req.Dimensions) != 2 || req.Dimensions[0] != "query" || req.Dimensions[1] != "page" {
		t.Fatalf("unexpected dimensions: %#v", req.Dimensions)
	}
	if len(req.DimensionFilterGroups) != 1 || len(req.DimensionFilterGroups[0].Filters) != 2 {
		t.Fatalf("unexpected filter groups: %#v", req.DimensionFilterGroups)
	}
}

func TestSearchConsoleSearchAnalyticsQueryCmd_BuildRequestFromJSON(t *testing.T) {
	withStdin(t, `{"startDate":"2026-02-01","endDate":"2026-02-10","rowLimit":50}`, func() {
		cmd := &SearchConsoleSearchAnalyticsQueryCmd{RequestPath: "-"}
		req, err := cmd.buildRequest()
		if err != nil {
			t.Fatalf("buildRequest: %v", err)
		}
		if req.RowLimit != 50 {
			t.Fatalf("unexpected rowLimit: %d", req.RowLimit)
		}
	})
}

func TestSearchConsoleSitesListCmd_JSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/sites") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"siteEntry": []map[string]any{
				{
					"siteUrl":         "https://example.com/",
					"permissionLevel": "siteOwner",
				},
			},
		})
	}))
	defer srv.Close()

	svc, err := webmasters.NewService(context.Background(), option.WithHTTPClient(srv.Client()), option.WithEndpoint(srv.URL+"/"))
	if err != nil {
		t.Fatalf("webmasters.NewService: %v", err)
	}

	orig := newSearchConsoleService
	newSearchConsoleService = func(context.Context, string) (*webmasters.Service, error) { return svc, nil }
	t.Cleanup(func() { newSearchConsoleService = orig })

	ctx := outfmt.WithMode(context.Background(), outfmt.Mode{JSON: true})
	out := captureStdout(t, func() {
		cmd := &SearchConsoleSitesListCmd{}
		if runErr := cmd.Run(ctx, &RootFlags{Account: "user@example.com"}); runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	})

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	sites, ok := parsed["sites"].([]any)
	if !ok || len(sites) != 1 {
		t.Fatalf("unexpected sites output: %#v", parsed["sites"])
	}
}

func TestSearchConsoleSearchAnalyticsQueryCmd_JSON(t *testing.T) {
	var gotBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/searchAnalytics/query") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"responseAggregationType": "byPage",
			"rows": []map[string]any{
				{
					"keys":        []string{"buy shoes", "https://example.com/shoes"},
					"clicks":      42,
					"impressions": 1010,
					"ctr":         0.0416,
					"position":    8.2,
				},
			},
		})
	}))
	defer srv.Close()

	svc, err := webmasters.NewService(context.Background(), option.WithHTTPClient(srv.Client()), option.WithEndpoint(srv.URL+"/"))
	if err != nil {
		t.Fatalf("webmasters.NewService: %v", err)
	}

	orig := newSearchConsoleService
	newSearchConsoleService = func(context.Context, string) (*webmasters.Service, error) { return svc, nil }
	t.Cleanup(func() { newSearchConsoleService = orig })

	ctx := outfmt.WithMode(context.Background(), outfmt.Mode{JSON: true})
	out := captureStdout(t, func() {
		cmd := &SearchConsoleSearchAnalyticsQueryCmd{
			SiteURL:    "sc-domain:example.com",
			StartDate:  "2026-02-01",
			EndDate:    "2026-02-07",
			Dimensions: "query,page",
			Limit:      5,
		}
		if runErr := cmd.Run(ctx, &RootFlags{Account: "user@example.com"}); runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	})

	if gotBody["startDate"] != "2026-02-01" || gotBody["endDate"] != "2026-02-07" {
		t.Fatalf("unexpected request body: %#v", gotBody)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	rows, ok := parsed["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("unexpected rows output: %#v", parsed["rows"])
	}
}
