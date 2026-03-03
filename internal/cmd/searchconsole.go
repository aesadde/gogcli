package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	gapi "google.golang.org/api/googleapi"
	"google.golang.org/api/webmasters/v3"

	"github.com/steipete/gogcli/internal/config"
	"github.com/steipete/gogcli/internal/googleapi"
	"github.com/steipete/gogcli/internal/outfmt"
	"github.com/steipete/gogcli/internal/ui"
)

var newSearchConsoleService = googleapi.NewSearchConsole

const (
	defaultSearchAnalyticsLimit = int64(1000)
	maxSearchAnalyticsLimit     = int64(5000)
)

type SearchConsoleCmd struct {
	Sites           SearchConsoleSitesCmd                `cmd:"" name:"sites" help:"List and inspect Search Console properties"`
	SearchAnalytics SearchConsoleSearchAnalyticsCmd      `cmd:"" name:"searchanalytics" aliases:"analytics" help:"Search Analytics queries"`
	Query           SearchConsoleSearchAnalyticsQueryCmd `cmd:"" name:"query" help:"Run a Search Analytics query (alias for 'searchconsole searchanalytics query')"`
	Sitemaps        SearchConsoleSitemapsCmd             `cmd:"" name:"sitemaps" help:"List/get/submit/delete sitemaps"`
}

type SearchConsoleSitesCmd struct {
	List SearchConsoleSitesListCmd `cmd:"" name:"list" aliases:"ls" help:"List accessible Search Console properties"`
	Get  SearchConsoleSitesGetCmd  `cmd:"" name:"get" help:"Get a specific Search Console property"`
}

type SearchConsoleSitesListCmd struct{}

func (c *SearchConsoleSitesListCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	svc, err := newSearchConsoleService(ctx, account)
	if err != nil {
		return err
	}

	resp, err := svc.Sites.List().Context(ctx).Do()
	if err != nil {
		return wrapSearchConsoleError(err)
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(ctx, os.Stdout, map[string]any{"sites": resp.SiteEntry})
	}

	if len(resp.SiteEntry) == 0 {
		u.Err().Println("No Search Console properties")
		return nil
	}

	w, flush := tableWriter(ctx)
	defer flush()

	_, _ = fmt.Fprintln(w, "SITE_URL\tPERMISSION")
	for _, site := range resp.SiteEntry {
		_, _ = fmt.Fprintf(w, "%s\t%s\n", site.SiteUrl, site.PermissionLevel)
	}
	return nil
}

type SearchConsoleSitesGetCmd struct {
	SiteURL string `arg:"" name:"site_url" help:"Property URI (for example https://www.example.com/ or sc-domain:example.com)"`
}

func (c *SearchConsoleSitesGetCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	siteURL := strings.TrimSpace(c.SiteURL)
	if siteURL == "" {
		return usage("empty site_url")
	}

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	svc, err := newSearchConsoleService(ctx, account)
	if err != nil {
		return err
	}

	site, err := svc.Sites.Get(siteURL).Context(ctx).Do()
	if err != nil {
		return wrapSearchConsoleError(err)
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(ctx, os.Stdout, map[string]any{"site": site})
	}

	return writeResult(ctx, u,
		kv("site_url", site.SiteUrl),
		kv("permission_level", site.PermissionLevel),
	)
}

type SearchConsoleSearchAnalyticsCmd struct {
	Query SearchConsoleSearchAnalyticsQueryCmd `cmd:"" name:"query" aliases:"run" help:"Run Search Analytics query"`
}

type SearchConsoleSearchAnalyticsQueryCmd struct {
	SiteURL string `arg:"" name:"site_url" help:"Property URI (for example https://www.example.com/ or sc-domain:example.com)"`

	StartDate       string   `name:"start" help:"Start date (YYYY-MM-DD)"`
	EndDate         string   `name:"end" help:"End date (YYYY-MM-DD)"`
	Dimensions      string   `name:"dimensions" help:"Comma-separated dimensions (query,page,country,device,searchAppearance,date,hour)"`
	SearchType      string   `name:"type" help:"Search type (web,image,video,news,discover,googleNews)" default:"web"`
	AggregationType string   `name:"aggregation" help:"Aggregation type (auto|byPage|byProperty|byNewsShowcasePanel)"`
	DataState       string   `name:"data-state" help:"Data state (final|all)"`
	Limit           int64    `name:"limit" help:"Row limit (1-5000)" default:"1000"`
	StartRow        int64    `name:"start-row" help:"Zero-based row offset" default:"0"`
	Filter          []string `name:"filter" help:"Dimension filter, repeatable: dimension:operator:expression"`
	FilterGroupType string   `name:"filter-group-type" help:"Filter group type for --filter values" enum:"and,or" default:"and"`
	RequestPath     string   `name:"request" help:"Path to SearchAnalyticsQueryRequest JSON ('-' for stdin). If set, request body is loaded from this file."`
}

func (c *SearchConsoleSearchAnalyticsQueryCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	siteURL := strings.TrimSpace(c.SiteURL)
	if siteURL == "" {
		return usage("empty site_url")
	}

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	req, err := c.buildRequest()
	if err != nil {
		return err
	}

	svc, err := newSearchConsoleService(ctx, account)
	if err != nil {
		return err
	}

	resp, err := svc.Searchanalytics.Query(siteURL, req).Context(ctx).Do()
	if err != nil {
		return wrapSearchConsoleError(err)
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(ctx, os.Stdout, map[string]any{
			"rows":                    resp.Rows,
			"responseAggregationType": resp.ResponseAggregationType,
		})
	}

	if len(resp.Rows) == 0 {
		u.Err().Println("No rows")
		return nil
	}

	dimensions := requestDimensions(req, resp.Rows)

	w, flush := tableWriter(ctx)
	defer flush()

	header := append([]string{}, dimensions...)
	header = append(header, "CLICKS", "IMPRESSIONS", "CTR", "POSITION")
	_, _ = fmt.Fprintln(w, strings.Join(header, "\t"))

	for _, row := range resp.Rows {
		values := make([]string, 0, len(dimensions)+4)
		for i := 0; i < len(dimensions); i++ {
			if i < len(row.Keys) {
				values = append(values, row.Keys[i])
			} else {
				values = append(values, "")
			}
		}

		values = append(
			values,
			formatMetric(row.Clicks, 0),
			formatMetric(row.Impressions, 0),
			formatMetric(row.Ctr, 4),
			formatMetric(row.Position, 2),
		)
		_, _ = fmt.Fprintln(w, strings.Join(values, "\t"))
	}

	return nil
}

func (c *SearchConsoleSearchAnalyticsQueryCmd) buildRequest() (*webmasters.SearchAnalyticsQueryRequest, error) {
	requestPath := strings.TrimSpace(c.RequestPath)
	if requestPath != "" {
		req, err := readSearchAnalyticsRequest(requestPath)
		if err != nil {
			return nil, err
		}
		if err := validateDateRange(req.StartDate, req.EndDate); err != nil {
			return nil, err
		}
		return req, nil
	}

	start := strings.TrimSpace(c.StartDate)
	end := strings.TrimSpace(c.EndDate)
	if start == "" || end == "" {
		return nil, usage("--start and --end are required unless --request is set")
	}
	if err := validateDateRange(start, end); err != nil {
		return nil, err
	}

	limit := c.Limit
	if limit == 0 {
		limit = defaultSearchAnalyticsLimit
	}
	if limit < 1 || limit > maxSearchAnalyticsLimit {
		return nil, usagef("invalid --limit %d (expected 1..5000)", limit)
	}
	if c.StartRow < 0 {
		return nil, usage("invalid --start-row (must be >= 0)")
	}

	req := &webmasters.SearchAnalyticsQueryRequest{
		StartDate: start,
		EndDate:   end,
		RowLimit:  limit,
		StartRow:  c.StartRow,
	}

	if dims := splitCommaList(c.Dimensions); len(dims) > 0 {
		req.Dimensions = dims
	}

	if v := strings.TrimSpace(c.SearchType); v != "" {
		req.SearchType = v
	}
	if v := strings.TrimSpace(c.AggregationType); v != "" {
		req.AggregationType = v
	}
	if v := strings.TrimSpace(c.DataState); v != "" {
		req.DataState = v
	}

	if len(c.Filter) > 0 {
		filters := make([]*webmasters.ApiDimensionFilter, 0, len(c.Filter))
		for _, raw := range c.Filter {
			filter, err := parseDimensionFilter(raw)
			if err != nil {
				return nil, err
			}
			filters = append(filters, filter)
		}

		req.DimensionFilterGroups = []*webmasters.ApiDimensionFilterGroup{
			{
				GroupType: strings.ToLower(strings.TrimSpace(c.FilterGroupType)),
				Filters:   filters,
			},
		}
	}

	return req, nil
}

type SearchConsoleSitemapsCmd struct {
	List   SearchConsoleSitemapsListCmd   `cmd:"" name:"list" aliases:"ls" help:"List sitemaps for a property"`
	Get    SearchConsoleSitemapsGetCmd    `cmd:"" name:"get" help:"Get a sitemap"`
	Submit SearchConsoleSitemapsSubmitCmd `cmd:"" name:"submit" help:"Submit a sitemap"`
	Delete SearchConsoleSitemapsDeleteCmd `cmd:"" name:"delete" aliases:"rm" help:"Delete a sitemap"`
}

type SearchConsoleSitemapsListCmd struct {
	SiteURL      string `arg:"" name:"site_url" help:"Property URI (for example https://www.example.com/ or sc-domain:example.com)"`
	SitemapIndex string `name:"sitemap-index" help:"Filter to a sitemap index URL"`
}

func (c *SearchConsoleSitemapsListCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	siteURL := strings.TrimSpace(c.SiteURL)
	if siteURL == "" {
		return usage("empty site_url")
	}

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	svc, err := newSearchConsoleService(ctx, account)
	if err != nil {
		return err
	}

	call := svc.Sitemaps.List(siteURL)
	if v := strings.TrimSpace(c.SitemapIndex); v != "" {
		call = call.SitemapIndex(v)
	}

	resp, err := call.Context(ctx).Do()
	if err != nil {
		return wrapSearchConsoleError(err)
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(ctx, os.Stdout, map[string]any{"sitemaps": resp.Sitemap})
	}

	if len(resp.Sitemap) == 0 {
		u.Err().Println("No sitemaps")
		return nil
	}

	w, flush := tableWriter(ctx)
	defer flush()

	_, _ = fmt.Fprintln(w, "PATH\tTYPE\tPENDING\tWARNINGS\tERRORS\tLAST_SUBMITTED\tLAST_DOWNLOADED\tCONTENTS")
	for _, sitemap := range resp.Sitemap {
		_, _ = fmt.Fprintf(
			w,
			"%s\t%s\t%t\t%d\t%d\t%s\t%s\t%s\n",
			sitemap.Path,
			sitemap.Type,
			sitemap.IsPending,
			sitemap.Warnings,
			sitemap.Errors,
			sitemap.LastSubmitted,
			sitemap.LastDownloaded,
			formatSitemapContents(sitemap.Contents),
		)
	}

	return nil
}

type SearchConsoleSitemapsGetCmd struct {
	SiteURL  string `arg:"" name:"site_url" help:"Property URI (for example https://www.example.com/ or sc-domain:example.com)"`
	FeedPath string `arg:"" name:"feed_path" help:"Sitemap URL"`
}

func (c *SearchConsoleSitemapsGetCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	siteURL := strings.TrimSpace(c.SiteURL)
	if siteURL == "" {
		return usage("empty site_url")
	}
	feedPath := strings.TrimSpace(c.FeedPath)
	if feedPath == "" {
		return usage("empty feed_path")
	}

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	svc, err := newSearchConsoleService(ctx, account)
	if err != nil {
		return err
	}

	sitemap, err := svc.Sitemaps.Get(siteURL, feedPath).Context(ctx).Do()
	if err != nil {
		return wrapSearchConsoleError(err)
	}

	if outfmt.IsJSON(ctx) {
		return outfmt.WriteJSON(ctx, os.Stdout, map[string]any{"sitemap": sitemap})
	}

	return writeResult(ctx, u,
		kv("path", sitemap.Path),
		kv("type", sitemap.Type),
		kv("pending", sitemap.IsPending),
		kv("warnings", sitemap.Warnings),
		kv("errors", sitemap.Errors),
		kv("last_submitted", sitemap.LastSubmitted),
		kv("last_downloaded", sitemap.LastDownloaded),
		kv("contents", formatSitemapContents(sitemap.Contents)),
	)
}

type SearchConsoleSitemapsSubmitCmd struct {
	SiteURL  string `arg:"" name:"site_url" help:"Property URI (for example https://www.example.com/ or sc-domain:example.com)"`
	FeedPath string `arg:"" name:"feed_path" help:"Sitemap URL"`
}

func (c *SearchConsoleSitemapsSubmitCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	siteURL := strings.TrimSpace(c.SiteURL)
	if siteURL == "" {
		return usage("empty site_url")
	}
	feedPath := strings.TrimSpace(c.FeedPath)
	if feedPath == "" {
		return usage("empty feed_path")
	}

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	if err := dryRunExit(ctx, flags, "searchconsole.sitemaps.submit", map[string]any{
		"site_url":  siteURL,
		"feed_path": feedPath,
	}); err != nil {
		return err
	}

	svc, err := newSearchConsoleService(ctx, account)
	if err != nil {
		return err
	}

	if err := svc.Sitemaps.Submit(siteURL, feedPath).Context(ctx).Do(); err != nil {
		return wrapSearchConsoleError(err)
	}

	return writeResult(ctx, u,
		kv("submitted", true),
		kv("site_url", siteURL),
		kv("feed_path", feedPath),
	)
}

type SearchConsoleSitemapsDeleteCmd struct {
	SiteURL  string `arg:"" name:"site_url" help:"Property URI (for example https://www.example.com/ or sc-domain:example.com)"`
	FeedPath string `arg:"" name:"feed_path" help:"Sitemap URL"`
}

func (c *SearchConsoleSitemapsDeleteCmd) Run(ctx context.Context, flags *RootFlags) error {
	u := ui.FromContext(ctx)
	siteURL := strings.TrimSpace(c.SiteURL)
	if siteURL == "" {
		return usage("empty site_url")
	}
	feedPath := strings.TrimSpace(c.FeedPath)
	if feedPath == "" {
		return usage("empty feed_path")
	}

	account, err := requireAccount(flags)
	if err != nil {
		return err
	}

	if err := confirmDestructive(ctx, flags, fmt.Sprintf("delete sitemap %s", feedPath)); err != nil {
		return err
	}

	svc, err := newSearchConsoleService(ctx, account)
	if err != nil {
		return err
	}

	if err := svc.Sitemaps.Delete(siteURL, feedPath).Context(ctx).Do(); err != nil {
		return wrapSearchConsoleError(err)
	}

	return writeResult(ctx, u,
		kv("deleted", true),
		kv("site_url", siteURL),
		kv("feed_path", feedPath),
	)
}

func validateDateRange(startDate, endDate string) error {
	startDate = strings.TrimSpace(startDate)
	endDate = strings.TrimSpace(endDate)
	if startDate == "" || endDate == "" {
		return usage("startDate and endDate are required")
	}

	start, err := parseDateOnly(startDate)
	if err != nil {
		return usagef("invalid start date %q (expected YYYY-MM-DD)", startDate)
	}
	end, err := parseDateOnly(endDate)
	if err != nil {
		return usagef("invalid end date %q (expected YYYY-MM-DD)", endDate)
	}
	if end.Before(start) {
		return usage("end date must be on or after start date")
	}
	return nil
}

func parseDateOnly(raw string) (time.Time, error) {
	return time.Parse("2006-01-02", strings.TrimSpace(raw))
}

func parseDimensionFilter(raw string) (*webmasters.ApiDimensionFilter, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, usage("empty --filter value")
	}

	first := strings.Index(raw, ":")
	last := strings.Index(raw[first+1:], ":")
	if first <= 0 || last < 0 {
		return nil, usagef("invalid --filter %q (expected dimension:operator:expression)", raw)
	}

	second := first + 1 + last
	dimension := strings.TrimSpace(raw[:first])
	operator := strings.TrimSpace(raw[first+1 : second])
	expression := strings.TrimSpace(raw[second+1:])
	if dimension == "" || operator == "" || expression == "" {
		return nil, usagef("invalid --filter %q (expected dimension:operator:expression)", raw)
	}

	opCanonical, err := normalizeFilterOperator(operator)
	if err != nil {
		return nil, err
	}

	return &webmasters.ApiDimensionFilter{
		Dimension:  dimension,
		Operator:   opCanonical,
		Expression: expression,
	}, nil
}

func normalizeFilterOperator(op string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "equals":
		return "equals", nil
	case "notequals":
		return "notEquals", nil
	case "contains":
		return "contains", nil
	case "notcontains":
		return "notContains", nil
	case "includingregex":
		return "includingRegex", nil
	case "excludingregex":
		return "excludingRegex", nil
	default:
		return "", usagef("invalid filter operator %q (expected equals|notEquals|contains|notContains|includingRegex|excludingRegex)", op)
	}
}

func readSearchAnalyticsRequest(path string) (*webmasters.SearchAnalyticsQueryRequest, error) {
	var (
		b   []byte
		err error
	)

	if path == "-" {
		b, err = io.ReadAll(os.Stdin)
	} else {
		expanded, expandErr := config.ExpandPath(path)
		if expandErr != nil {
			return nil, expandErr
		}
		b, err = os.ReadFile(expanded) //nolint:gosec // user-provided path
	}
	if err != nil {
		return nil, err
	}

	var req webmasters.SearchAnalyticsQueryRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return nil, fmt.Errorf("decode search analytics request: %w", err)
	}

	if req.RowLimit == 0 {
		req.RowLimit = defaultSearchAnalyticsLimit
	}
	if req.RowLimit < 1 || req.RowLimit > maxSearchAnalyticsLimit {
		return nil, usagef("invalid request.rowLimit %d (expected 1..5000)", req.RowLimit)
	}
	if req.StartRow < 0 {
		return nil, usage("invalid request.startRow (must be >= 0)")
	}

	return &req, nil
}

func requestDimensions(req *webmasters.SearchAnalyticsQueryRequest, rows []*webmasters.ApiDataRow) []string {
	if len(req.Dimensions) > 0 {
		out := make([]string, 0, len(req.Dimensions))
		for _, dim := range req.Dimensions {
			out = append(out, strings.ToUpper(strings.TrimSpace(dim)))
		}
		return out
	}

	keyCount := 0
	for _, row := range rows {
		if len(row.Keys) > keyCount {
			keyCount = len(row.Keys)
		}
	}

	out := make([]string, 0, keyCount)
	for i := 0; i < keyCount; i++ {
		out = append(out, "KEY_"+strconv.Itoa(i+1))
	}
	return out
}

func formatMetric(v float64, decimals int) string {
	if decimals <= 0 {
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return strconv.FormatFloat(v, 'f', decimals, 64)
}

func formatSitemapContents(contents []*webmasters.WmxSitemapContent) string {
	if len(contents) == 0 {
		return ""
	}

	parts := make([]string, 0, len(contents))
	for _, content := range contents {
		parts = append(parts, fmt.Sprintf("%s:%d/%d", content.Type, content.Indexed, content.Submitted))
	}
	return strings.Join(parts, ",")
}

func wrapSearchConsoleError(err error) error {
	var apiErr *gapi.Error
	if !errors.As(err, &apiErr) {
		return err
	}

	if apiErr.Code != 403 {
		return err
	}

	message := strings.ToLower(apiErr.Message)
	switch {
	case strings.Contains(message, "accessnotconfigured"), strings.Contains(message, "api has not been used"):
		return fmt.Errorf("Search Console API is not enabled for this OAuth project. Enable it at https://console.cloud.google.com/apis/api/searchconsole.googleapis.com")
	case strings.Contains(message, "insufficientpermissions"), strings.Contains(message, "insufficient permission"):
		return fmt.Errorf("insufficient permissions for Search Console API. Re-authorize with: gog auth add <email> --services searchconsole")
	default:
		return err
	}
}
