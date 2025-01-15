package orgstats

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/caarlos0/org-stats/github_errors"

	"github.com/google/go-github/v39/github"
)

// Stat represents an user adds, rms and commits count
type Stat struct {
	Additions, Deletions int
	Commits, Reviews int
	PullRequests int
}

// Stats contains the user->Stat mapping
type Stats struct {
	data  map[string]Stat
	since time.Time
}

func (s Stats) Logins() []string {
	logins := make([]string, 0, len(s.data))
	for login := range s.data {
		logins = append(logins, login)
	}
	return logins
}

func (s Stats) For(login string) Stat {
	return s.data[login]
}

// NewStats return a new Stats map
func NewStats(since time.Time) Stats {
	return Stats{
		data:  make(map[string]Stat),
		since: since,
	}
}

// Gather a given organization's stats
func Gather(
	ctx context.Context,
	client *github.Client,
	org string,
	userBlacklist, repoBlacklist []string,
	since time.Time,
	includeReviewStats bool,
	excludeForks bool,
) (Stats, error) {

	allStats := NewStats(since)
	if err := gatherLineStats(
		ctx,
		client,
		org,
		userBlacklist,
		repoBlacklist,
		excludeForks,
		&allStats,
	); err != nil {
		return Stats{}, err
	}

	if err := gatherPullRequestStats(
		ctx,
		client,
		org,
		userBlacklist,
		repoBlacklist,
		&allStats,
		since,
	); err != nil {
		return Stats{}, err
	}

	log.Println("total authors stats:", len(allStats.data))

	if !includeReviewStats {
		return allStats, nil
	}

	for user := range allStats.data {
		log.Println("gathering review stats for user:", user)
		if err := gatherReviewStats(
			ctx,
			client,
			org,
			user,
			userBlacklist,
			repoBlacklist,
			&allStats,
			since,
		); err != nil {
			return Stats{}, err
		}
	}

	return allStats, nil
}

func gatherReviewStats(
	ctx context.Context,
	client *github.Client,
	org, user string,
	userBlacklist, repoBlacklist []string,
	allStats *Stats,
	since time.Time,
) error {
	query := fmt.Sprintf("user:%s is:pr reviewed-by:%s", org, user)
	if !since.IsZero() {
		query += fmt.Sprintf(" created:>%s", since.Format("2006-01-02"))
	}
	reviewed, err := search(ctx, client, query)
	if err != nil {
		log.Printf("[ERROR] Failed to gather review stats for user %s: %v", user, err)
		return err
	}
	allStats.addReviewStats(user, reviewed)
	return nil
}

func search(
	ctx context.Context,
	client *github.Client,
	query string,
) (int, error) {
	log.Printf("searching '%s'", query)
	result, resp, err := client.Search.Issues(ctx, query, &github.SearchOptions{
		ListOptions: github.ListOptions{
			PerPage: 1,
		},
	})
	if rateErr, ok := err.(*github.RateLimitError); ok {
		handleRateLimit(rateErr)
		return search(ctx, client, query)
	}
	if isSecondRateErr, secondRateErr := githuberrors.IsSecondaryRateLimitError(resp); isSecondRateErr {
		handleSecondaryRateLimit(secondRateErr)
		return search(ctx, client, query)
	}
	if _, ok := err.(*github.AcceptedError); ok {
		return search(ctx, client, query)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to search: %s: %w", query, err)
	}
	return *result.Total, nil
}

func gatherLineStats(
	ctx context.Context,
	client *github.Client,
	org string,
	userBlacklist, repoBlacklist []string,
	excludeForks bool,
	allStats *Stats,
) error {
	allRepos, err := repos(ctx, client, org)
	if err != nil {
		log.Printf("[ERROR] Failed to get repos: %v", err)
		return err
	}

	for _, repo := range allRepos {
		if excludeForks && *repo.Fork {
			log.Println("ignoring forked repo:", repo.GetName())
			continue
		}
		if isBlacklisted(repoBlacklist, repo.GetName()) {
			log.Println("ignoring blacklisted repo:", repo.GetName())
			continue
		}
		stats, serr := getStats(ctx, client, org, *repo.Name)
		if serr != nil {
			log.Printf("[ERROR] Failed to get stats for repo %s: %v", *repo.Name, serr)
			return serr
		}
		for _, cs := range stats {
			if isBlacklisted(userBlacklist, cs.Author.GetLogin()) {
				log.Println("ignoring blacklisted author:", cs.Author.GetLogin())
				continue
			}
			log.Println("recording stats for author", cs.Author.GetLogin(), "on repo", repo.GetName())
			allStats.add(cs)
		}
	}
	return err
}

func isBlacklisted(blacklist []string, s string) bool {
	for _, b := range blacklist {
		if strings.EqualFold(s, b) {
			return true
		}
	}
	return false
}

func (s *Stats) addReviewStats(user string, reviewed int) {
	stat := s.data[user]
	stat.Reviews += reviewed
	s.data[user] = stat
}

func (s *Stats) add(cs *github.ContributorStats) {
	if cs.GetAuthor() == nil {
		return
	}
	stat := s.data[cs.GetAuthor().GetLogin()]
	var adds int
	var rms int
	var commits int
	for _, week := range cs.Weeks {
		if !s.since.IsZero() && week.Week.Time.UTC().Before(s.since) {
			continue
		}
		adds += *week.Additions
		rms += *week.Deletions
		commits += *week.Commits
	}
	stat.Additions += adds
	stat.Deletions += rms
	stat.Commits += commits
	if stat.Additions+stat.Deletions+stat.Commits == 0 && !s.since.IsZero() {
		// ignore users with no activity when running with a since time
		return
	}
	s.data[cs.GetAuthor().GetLogin()] = stat
}

func repos(ctx context.Context, client *github.Client, org string) ([]*github.Repository, error) {
	opt := &github.RepositoryListByOrgOptions{
		ListOptions: github.ListOptions{PerPage: 10},
	}
	var allRepos []*github.Repository
	for {
		repos, resp, err := client.Repositories.ListByOrg(ctx, org, opt)
		if rateErr, ok := err.(*github.RateLimitError); ok {
			handleRateLimit(rateErr)
			continue
		}
		if isSecondRateErr, secondRateErr := githuberrors.IsSecondaryRateLimitError(resp); isSecondRateErr {
			handleSecondaryRateLimit(secondRateErr)
			continue
		}
		if err != nil {
			return allRepos, err
		}
		allRepos = append(allRepos, repos...)
		if resp.NextPage == 0 {
			break
		}
		opt.ListOptions.Page = resp.NextPage
	}

	log.Println("got", len(allRepos), "repositories")
	return allRepos, nil
}

func getStats(ctx context.Context, client *github.Client, org, repo string) ([]*github.ContributorStats, error) {
	stats, resp, err := client.Repositories.ListContributorsStats(ctx, org, repo)
	if err != nil {
		if rateErr, ok := err.(*github.RateLimitError); ok {
			handleRateLimit(rateErr)
			return getStats(ctx, client, org, repo)
		}
		if isSecondRateErr, secondRateErr := githuberrors.IsSecondaryRateLimitError(resp); isSecondRateErr {
			handleSecondaryRateLimit(secondRateErr)
			return getStats(ctx, client, org, repo)
		}
		if _, ok := err.(*github.AcceptedError); ok {
			return getStats(ctx, client, org, repo)
		}
	}
	return stats, err
}

func handleRateLimit(err *github.RateLimitError) {
	s := err.Rate.Reset.UTC().Sub(time.Now().UTC())
	if s < 0 {
		s = 5 * time.Second
	}
	log.Printf("hit rate limit, waiting %v", s)
	time.Sleep(s)
}

func handleSecondaryRateLimit(err *githuberrors.SecondaryRateLimitError) {
	s := err.RetryAfter.UTC().Sub(time.Now().UTC())
	if s < 0 {
		s = 10 * time.Second
	}
	log.Printf("hit secondary rate limit, waiting %v", s)
	time.Sleep(s)
}

func gatherPullRequestStats(
	ctx context.Context,
	client *github.Client,
	org string,
	userBlacklist, repoBlacklist []string,
	allStats *Stats,
	since time.Time,
) error {
	query := fmt.Sprintf("org:%s is:pr", org)
	if !since.IsZero() {
		query += fmt.Sprintf(" created:>%s", since.Format("2006-01-02"))
	}
	
	opt := &github.SearchOptions{
		ListOptions: github.ListOptions{PerPage: 100},
	}
	
	for {
		result, resp, err := client.Search.Issues(ctx, query, opt)
		if rateErr, ok := err.(*github.RateLimitError); ok {
			handleRateLimit(rateErr)
			continue
		}
		if isSecondRateErr, secondRateErr := githuberrors.IsSecondaryRateLimitError(resp); isSecondRateErr {
			handleSecondaryRateLimit(secondRateErr)
			continue
		}
		if err != nil {
			log.Printf("[ERROR] Failed to search PRs: %v", err)
			return fmt.Errorf("failed to search PRs: %w", err)
		}

		for _, pr := range result.Issues {
			if isBlacklisted(userBlacklist, pr.GetUser().GetLogin()) {
				log.Println("ignoring blacklisted PR author:", pr.GetUser().GetLogin())
				continue
			}

			// Get PR details to access the changed files
			prDetails, _, err := client.PullRequests.Get(ctx, org, getRepoFromPRURL(pr.GetHTMLURL()), pr.GetNumber())
			if err != nil {
				log.Printf("[ERROR] Failed to get PR details for org=%s repo=%s pr=%d: %v", 
					org, 
					getRepoFromPRURL(pr.GetHTMLURL()), 
					pr.GetNumber(), 
					err,
				)
				continue
			}

			if isBlacklisted(repoBlacklist, getRepoFromPRURL(pr.GetHTMLURL())) {
				log.Printf("[INFO] Ignoring blacklisted repo: org=%s repo=%s", 
					org, 
					getRepoFromPRURL(pr.GetHTMLURL()),
				)
				continue
			}

			log.Printf("[INFO] Recording PR stats for org=%s repo=%s author=%s pr=%d", 
				org,
				getRepoFromPRURL(pr.GetHTMLURL()),
				pr.GetUser().GetLogin(), 
				pr.GetNumber(),
			)
			allStats.addPRStats(pr.GetUser().GetLogin(), prDetails.GetAdditions(), prDetails.GetDeletions(), 1)
		}

		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	
	return nil
}

func getRepoFromPRURL(url string) string {
	parts := strings.Split(url, "/")
	if len(parts) >= 5 {
		return parts[4]
	}
	return ""
}

func (s *Stats) addPRStats(user string, additions, deletions, commits int) {
	stat := s.data[user]
	stat.Additions += additions
	stat.Deletions += deletions
	stat.Commits += commits
	stat.PullRequests += 1
	s.data[user] = stat
}
