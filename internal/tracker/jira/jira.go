// Package jira implements [domain.TrackerAdapter] for Atlassian Jira
// Cloud REST API v3 and Server/Data Center REST API v2. Issues are
// fetched via JQL search, normalized to domain types with ADF
// descriptions (v3) or plain-text descriptions (v2) flattened to
// plain text, labels lowercased, integer-only priority (non-integers
// become nil), and blocker refs extracted from inward "Blocks"
// issuelinks. Registered under kind "jira" via an init function.
package jira

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/httpkit"
	"github.com/sortie-ai/sortie/internal/registry"
	"github.com/sortie-ai/sortie/internal/trackermetrics"
	"github.com/sortie-ai/sortie/internal/typeutil"
)

func init() {
	registry.Trackers.RegisterWithMeta("jira", NewJiraAdapter, registry.TrackerMeta{
		RequiresProject: true,
		RequiresAPIKey:  true,
	})
}

// Compile-time interface satisfaction check.
var _ domain.TrackerAdapter = (*JiraAdapter)(nil)

// searchFields is the comma-separated list of Jira fields requested
// in search and issue-detail operations.
const searchFields = "summary,status,priority,labels,assignee,issuetype,parent,issuelinks,created,updated,description"

// defaultActiveStates is applied when the config omits active_states.
var defaultActiveStates = []string{"Backlog", "Selected for Development", "In Progress"}

// maxSearchResults is the page size for search pagination (cursor-based
// for v3, offset-based for v2).
const maxSearchResults = "50"

// maxCommentResults is the page size for offset-based comment pagination.
const maxCommentResults = 50

// batchSize is the maximum number of issue keys per JQL IN clause
// to keep GET URLs within safe URI length limits.
const batchSize = 40

// JiraAdapter implements [domain.TrackerAdapter] against Jira Cloud
// REST API v3 and Server/Data Center REST API v2. Safe for concurrent
// use.
type JiraAdapter struct {
	client       *httpkit.Client
	project      string
	activeStates []string
	endpoint     string
	apiVersion   string
	queryFilter  string
	metrics      domain.Metrics // nil-safe: check before calling
}

// NewJiraAdapter creates a [JiraAdapter] from adapter configuration.
// Required config keys: "endpoint", "api_key", "project". Optional:
// "api_version" ("2" or "3", default "3"), "active_states" (defaults
// to Backlog, Selected for Development, In Progress), "query_filter"
// (raw JQL fragment).
//
// For api_version "3" (Cloud), api_key must be in email:token format
// and Basic auth is used. For api_version "2" (Server/Data Center),
// api_key may be a bare Personal Access Token (Bearer auth) or
// user:password (Basic auth).
func NewJiraAdapter(config map[string]any) (domain.TrackerAdapter, error) {
	endpoint, _ := config["endpoint"].(string)
	if endpoint == "" {
		return nil, &domain.TrackerError{
			Kind:    domain.ErrTrackerPayload,
			Message: "missing required config key: endpoint",
		}
	}
	endpoint = strings.TrimRight(endpoint, "/")
	if strings.Contains(endpoint, "/rest/api/") {
		return nil, &domain.TrackerError{
			Kind:    domain.ErrTrackerPayload,
			Message: `endpoint must be the Jira base URL without "/rest/api/..." path`,
		}
	}

	apiVersion, _ := config["api_version"].(string)
	if apiVersion == "" {
		apiVersion = "3"
	}
	if apiVersion != "2" && apiVersion != "3" {
		return nil, &domain.TrackerError{
			Kind:    domain.ErrTrackerPayload,
			Message: fmt.Sprintf("api_version must be %q or %q, got %q", "2", "3", apiVersion),
		}
	}

	apiKey, _ := config["api_key"].(string)
	if apiKey == "" {
		return nil, &domain.TrackerError{
			Kind:    domain.ErrMissingTrackerAPIKey,
			Message: "missing required config key: api_key",
		}
	}

	var email, token, authScheme string
	idx := strings.Index(apiKey, ":")
	if apiVersion == "3" {
		// v3 (Cloud): require email:token format, always Basic auth.
		if idx < 1 || idx == len(apiKey)-1 {
			return nil, &domain.TrackerError{
				Kind:    domain.ErrTrackerAuth,
				Message: "api_key must be in email:token format",
			}
		}
		email = apiKey[:idx]
		token = apiKey[idx+1:]
		authScheme = "basic"
	} else {
		// v2 (Server/DC): colon present means user:password (Basic),
		// bare token means PAT (Bearer).
		if idx >= 1 && idx < len(apiKey)-1 {
			email = apiKey[:idx]
			token = apiKey[idx+1:]
			authScheme = "basic"
		} else {
			token = apiKey
			authScheme = "bearer"
		}
	}

	project, _ := config["project"].(string)
	if project == "" {
		return nil, &domain.TrackerError{
			Kind:    domain.ErrMissingTrackerProject,
			Message: "missing required config key: project",
		}
	}

	activeStates := typeutil.ExtractStringSlice(config["active_states"])
	if len(activeStates) == 0 {
		activeStates = defaultActiveStates
	}

	queryFilter, _ := config["query_filter"].(string)

	userAgent, _ := config["user_agent"].(string)
	if userAgent == "" {
		userAgent = "sortie/dev"
	}

	return &JiraAdapter{
		client:       newJiraClient(endpoint, email, token, userAgent, authScheme),
		project:      project,
		activeStates: activeStates,
		endpoint:     endpoint,
		apiVersion:   apiVersion,
		queryFilter:  queryFilter,
	}, nil
}

// apiPath returns the versioned REST API path for the given subpath.
func (a *JiraAdapter) apiPath(subpath string) string {
	return "/rest/api/" + a.apiVersion + "/" + subpath
}

// isV2 reports whether the adapter is configured for Jira REST API v2.
func (a *JiraAdapter) isV2() bool {
	return a.apiVersion == "2"
}

// FetchCandidateIssues returns issues in configured active states
// for the configured project. Comments are set to nil on all returned
// issues. Results are ordered by priority then creation time.
func (a *JiraAdapter) FetchCandidateIssues(ctx context.Context) ([]domain.Issue, error) {
	issues := make([]domain.Issue, 0)
	err := trackermetrics.Track(a.metrics, "fetch_candidates", func() error {
		var fetchErr error
		jql := buildCandidateJQL(a.project, a.activeStates, a.queryFilter)
		issues, fetchErr = a.paginatedSearch(ctx, jql, searchFields)
		return fetchErr
	})
	return issues, err
}

// FetchIssueByID returns a fully populated issue including comments.
// The issueID is the Jira issue key (e.g. "PROJ-123").
func (a *JiraAdapter) FetchIssueByID(ctx context.Context, issueID string) (domain.Issue, error) {
	var issue domain.Issue
	err := trackermetrics.Track(a.metrics, "fetch_issue", func() error {
		params := url.Values{"fields": {searchFields}}
		body, _, err := a.client.Get(ctx, a.apiPath("issue/"+url.PathEscape(issueID)), params)
		if err != nil {
			if domain.IsNotFound(err) {
				return &domain.TrackerError{
					Kind:    domain.ErrTrackerNotFound,
					Message: fmt.Sprintf("issue not found: %s", issueID),
				}
			}
			return err
		}

		var ji jiraIssue
		if err := json.Unmarshal(body, &ji); err != nil {
			return &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: "failed to parse issue response",
				Err:     err,
			}
		}

		fetchedIssue := normalizeSearchIssue(a.endpoint, ji, !a.isV2())

		comments, err := a.fetchComments(ctx, issueID)
		if err != nil {
			return err
		}
		fetchedIssue.Comments = comments
		issue = fetchedIssue
		return nil
	})
	return issue, err
}

// FetchIssuesByStates returns issues in the specified states. Used
// for startup terminal cleanup. Returns immediately when states is
// empty.
func (a *JiraAdapter) FetchIssuesByStates(ctx context.Context, states []string) ([]domain.Issue, error) {
	if len(states) == 0 {
		return []domain.Issue{}, nil
	}

	issues := make([]domain.Issue, 0)
	err := trackermetrics.Track(a.metrics, "fetch_by_states", func() error {
		var fetchErr error
		jql := buildStatesFetchJQL(a.project, states, a.queryFilter)
		issues, fetchErr = a.paginatedSearch(ctx, jql, searchFields)
		return fetchErr
	})
	return issues, err
}

// FetchIssueStatesByIDs returns the current state for each requested
// issue ID (Jira internal numeric ID). Issues not found are omitted
// from the result map. Batches IDs into groups of 40 to stay within
// URI length limits. The queryFilter is intentionally not applied.
func (a *JiraAdapter) FetchIssueStatesByIDs(ctx context.Context, issueIDs []string) (map[string]string, error) {
	if len(issueIDs) == 0 {
		return map[string]string{}, nil
	}

	var statesByID map[string]string
	err := trackermetrics.Track(a.metrics, "fetch_states_by_ids", func() error {
		fetchedStates := make(map[string]string, len(issueIDs))
		for start := 0; start < len(issueIDs); start += batchSize {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			end := min(start+batchSize, len(issueIDs))
			batch := issueIDs[start:end]

			jql := buildIDINJQL(batch)
			if jql == "" {
				continue
			}

			issues, err := a.paginatedSearch(ctx, jql, "status")
			if err != nil {
				return err
			}
			for _, iss := range issues {
				fetchedStates[iss.ID] = iss.State
			}
		}
		statesByID = fetchedStates
		return nil
	})
	return statesByID, err
}

// FetchIssueStatesByIdentifiers returns the current state for each
// requested issue identifier (key). Batches identifiers into groups
// of 40 to stay within URI length limits. Issues not found are
// omitted from the result map. The queryFilter is intentionally not
// applied.
func (a *JiraAdapter) FetchIssueStatesByIdentifiers(ctx context.Context, identifiers []string) (map[string]string, error) {
	if len(identifiers) == 0 {
		return map[string]string{}, nil
	}

	var statesByKey map[string]string
	err := trackermetrics.Track(a.metrics, "fetch_states_by_identifiers", func() error {
		fetchedStates := make(map[string]string, len(identifiers))
		for start := 0; start < len(identifiers); start += batchSize {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			end := min(start+batchSize, len(identifiers))
			batch := identifiers[start:end]

			jql := buildKeyINJQL(batch)
			issues, err := a.paginatedSearch(ctx, jql, "status")
			if err != nil {
				return err
			}
			for _, iss := range issues {
				fetchedStates[iss.Identifier] = iss.State
			}
		}
		statesByKey = fetchedStates
		return nil
	})
	return statesByKey, err
}

// FetchIssueComments returns comments for the specified issue.
// Returns an empty non-nil slice when no comments exist.
func (a *JiraAdapter) FetchIssueComments(ctx context.Context, issueID string) ([]domain.Comment, error) {
	comments := make([]domain.Comment, 0)
	err := trackermetrics.Track(a.metrics, "fetch_comments", func() error {
		var fetchErr error
		comments, fetchErr = a.fetchComments(ctx, issueID)
		return fetchErr
	})
	return comments, err
}

// TransitionIssue moves an issue to the specified target state by
// finding and executing the matching Jira workflow transition.
// Available transitions are fetched via GET, matched by target status
// name (case-insensitive, first match), then executed via POST.
func (a *JiraAdapter) TransitionIssue(ctx context.Context, issueID string, targetState string) error {
	return trackermetrics.Track(a.metrics, "transition", func() error {
		path := a.apiPath("issue/" + url.PathEscape(issueID) + "/transitions")

		body, _, err := a.client.Get(ctx, path, nil)
		if err != nil {
			return err
		}

		var resp transitionsResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: fmt.Sprintf("failed to parse transitions response for issue %s", issueID),
				Err:     err,
			}
		}

		var matchID string
		for _, t := range resp.Transitions {
			if strings.EqualFold(t.To.Name, targetState) {
				matchID = t.ID
				break
			}
		}

		if matchID == "" {
			return &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: fmt.Sprintf("no transition to state %q available for issue %s", targetState, issueID),
			}
		}

		postBody, err := json.Marshal(struct {
			Transition struct {
				ID string `json:"id"`
			} `json:"transition"`
		}{
			Transition: struct {
				ID string `json:"id"`
			}{ID: matchID},
		})
		if err != nil {
			return &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: "failed to marshal transition request",
				Err:     err,
			}
		}

		_, err = a.client.Send(ctx, "POST", path, bytes.NewReader(postBody))
		return err
	})
}

// CommentIssue posts a plain-text comment on the specified Jira issue.
// For v3, the text is wrapped in ADF paragraph nodes. For v2, it is
// posted as a plain-text body string.
func (a *JiraAdapter) CommentIssue(ctx context.Context, issueID string, text string) error {
	return trackermetrics.Track(a.metrics, "comment", func() error {
		path := a.apiPath("issue/" + url.PathEscape(issueID) + "/comment")

		var commentBody any
		if a.isV2() {
			commentBody = map[string]string{"body": text}
		} else {
			commentBody = buildADFComment(text)
		}
		payload, err := json.Marshal(commentBody)
		if err != nil {
			return &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: "failed to marshal comment request",
				Err:     err,
			}
		}

		_, err = a.client.Send(ctx, "POST", path, bytes.NewReader(payload))
		return err
	})
}

// SetMetrics configures the metrics recorder for tracker API call
// instrumentation. When not called or called with nil, the adapter
// operates without recording metrics. Safe to call before any
// adapter operations. Not safe to call concurrently with adapter
// operations.
func (a *JiraAdapter) SetMetrics(m domain.Metrics) {
	a.metrics = m
}

// AddLabel adds a label to the specified issue via the Jira REST API.
// Returns an error if the request fails; the orchestrator treats AddLabel
// errors as non-fatal.
func (a *JiraAdapter) AddLabel(ctx context.Context, issueID string, label string) error {
	path := a.apiPath("issue/" + url.PathEscape(issueID))

	payload, err := json.Marshal(map[string]any{
		"update": map[string]any{
			"labels": []map[string]any{
				{"add": label},
			},
		},
	})
	if err != nil {
		a.incTrackerRequest("add_label", "error")
		return &domain.TrackerError{
			Kind:    domain.ErrTrackerPayload,
			Message: "failed to marshal label payload",
			Err:     err,
		}
	}

	_, err = a.client.Send(ctx, "PUT", path, bytes.NewReader(payload))
	if err != nil {
		a.incTrackerRequest("add_label", "error")
		return err
	}
	a.incTrackerRequest("add_label", "success")
	return nil
}

func (a *JiraAdapter) incTrackerRequest(operation, result string) {
	if a.metrics != nil {
		a.metrics.IncTrackerRequests(operation, result)
	}
}

// paginatedSearch executes a paginated JQL search and returns all
// normalized issues. Comments are set to nil. Dispatches to
// cursor-based (v3) or offset-based (v2) pagination.
func (a *JiraAdapter) paginatedSearch(ctx context.Context, jql, fields string) ([]domain.Issue, error) {
	if a.isV2() {
		return a.paginatedSearchV2(ctx, jql, fields)
	}
	return a.paginatedSearchV3(ctx, jql, fields)
}

// paginatedSearchV3 uses cursor-based pagination on /rest/api/3/search/jql.
func (a *JiraAdapter) paginatedSearchV3(ctx context.Context, jql, fields string) ([]domain.Issue, error) {
	params := url.Values{
		"jql":        {jql},
		"fields":     {fields},
		"maxResults": {maxSearchResults},
	}

	paginator := httpkit.NewTokenPaginator(a.client, a.apiPath("search/jql"), params, "nextPageToken", func(body []byte) ([]domain.Issue, string, error) {
		var sr searchResponse
		if err := json.Unmarshal(body, &sr); err != nil {
			return nil, "", &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: "failed to parse search response",
				Err:     err,
			}
		}

		issues := make([]domain.Issue, 0, len(sr.Issues))
		for _, ji := range sr.Issues {
			issue := normalizeSearchIssue(a.endpoint, ji, true)
			issue.Comments = nil
			issues = append(issues, issue)
		}
		return issues, sr.NextPageToken, nil
	}, httpkit.PaginatorOptions{})

	return paginator.All(ctx)
}

// paginatedSearchV2 uses offset-based pagination on /rest/api/2/search.
func (a *JiraAdapter) paginatedSearchV2(ctx context.Context, jql, fields string) ([]domain.Issue, error) {
	var allIssues []domain.Issue
	startAt := 0

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		params := url.Values{
			"jql":        {jql},
			"fields":     {fields},
			"maxResults": {maxSearchResults},
			"startAt":    {fmt.Sprintf("%d", startAt)},
		}

		body, _, err := a.client.Get(ctx, a.apiPath("search"), params)
		if err != nil {
			return nil, err
		}

		var sr searchResponseV2
		if err := json.Unmarshal(body, &sr); err != nil {
			return nil, &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: "failed to parse search response",
				Err:     err,
			}
		}

		for _, ji := range sr.Issues {
			issue := normalizeSearchIssue(a.endpoint, ji, false)
			issue.Comments = nil
			allIssues = append(allIssues, issue)
		}

		if len(sr.Issues) == 0 || startAt+len(sr.Issues) >= sr.Total {
			break
		}
		startAt += len(sr.Issues)
	}

	if allIssues == nil {
		return []domain.Issue{}, nil
	}
	return allIssues, nil
}

// fetchComments retrieves all comments for an issue using offset-based
// pagination. Returns an empty non-nil slice when no comments exist.
func (a *JiraAdapter) fetchComments(ctx context.Context, issueID string) ([]domain.Comment, error) {
	var allComments []jiraComment
	startAt := 0

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		params := url.Values{
			"orderBy":    {"created"},
			"maxResults": {fmt.Sprintf("%d", maxCommentResults)},
			"startAt":    {fmt.Sprintf("%d", startAt)},
		}

		body, _, err := a.client.Get(ctx, a.apiPath("issue/"+url.PathEscape(issueID)+"/comment"), params)
		if err != nil {
			if domain.IsNotFound(err) {
				return nil, &domain.TrackerError{
					Kind:    domain.ErrTrackerNotFound,
					Message: fmt.Sprintf("issue not found: %s", issueID),
				}
			}
			return nil, err
		}

		var cr commentResponse
		if err := json.Unmarshal(body, &cr); err != nil {
			return nil, &domain.TrackerError{
				Kind:    domain.ErrTrackerPayload,
				Message: "failed to parse comment response",
				Err:     err,
			}
		}

		allComments = append(allComments, cr.Comments...)

		if len(cr.Comments) == 0 || startAt+len(cr.Comments) >= cr.Total {
			break
		}
		startAt += len(cr.Comments)
	}

	if len(allComments) == 0 {
		return []domain.Comment{}, nil
	}
	return normalizeComments(allComments, !a.isV2()), nil
}
