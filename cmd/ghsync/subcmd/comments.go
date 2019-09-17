package subcmd

import (
	"context"
	"database/sql"
	"strings"

	"github.com/google/go-github/github"
	"github.com/src-d/ghsync/models"
	"github.com/src-d/ghsync/utils"
	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-kallax.v1"
	"gopkg.in/src-d/go-log.v1"
)

type CommentsCommand struct {
	cli.Command `name:"comments" short-description:"Deep sync issue comments from GitHub data" long-description:"Deep sync issue comments from GitHub data"`

	Tokens        string `long:"tokens" env:"GHSYNC_TOKENS" description:"GitHub personal access tokens comma separated" required:"true"`
	Org           string `long:"org" env:"GHSYNC_ORG" description:"Name of the GitHub organization" required:"true"`
	ExcludedRepos string `long:"excluded-repos" env:"GHSYNC_EXCLUDED_REPOS" description:"Excluded repos from sync comma separated"`

	Postgres PostgresOpt `group:"PostgreSQL connection options"`

	client *utils.WrapperClient
	store  *models.IssueCommentStore
}

func (c *CommentsCommand) Execute(args []string) error {
	db, err := c.Postgres.initDB()
	if err != nil {
		return err
	}
	defer db.Close()

	tokens := strings.Split(c.Tokens, ",")
	client, err := utils.NewWrapperClient(tokens)
	if err != nil {
		return err
	}

	excludedRepos := strings.Split(c.ExcludedRepos, ",")

	c.client = client
	c.store = models.NewIssueCommentStore(db)

	logger := log.New(log.Fields{"owner": c.Org})

	if err := c.getAllIssuesComments(logger, db, c.Org, excludedRepos); err != nil {
		return err
	}

	return nil
}

func (c *CommentsCommand) getAllIssuesComments(logger log.Logger, db *sql.DB, owner string, excludedRepos []string) error {
	logger.Infof("getting all issues comments")
	logger.Infof("excluding %d repos: %v", len(excludedRepos), excludedRepos)
	repos, err := c.getRepositories(db, excludedRepos)
	if err != nil {
		return err
	}

	totalRepos := len(repos)
	logger.Infof("found %d repos", totalRepos)
	for i, r := range repos {
		logger.Infof("[%d/%d] getting all issues comments for repo: %s",
			i+1, totalRepos, r)
		err := c.getIssueCommentsForRepo(logger, db, owner, r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CommentsCommand) getIssueCommentsForRepo(logger log.Logger, db *sql.DB, owner string, repo string) error {
	logger = logger.With(log.Fields{"repo": repo})

	logger.Infof("getting all issues numbers for repo")
	issueNumbers, err := c.getIssuesNumbersForRepo(db, owner, repo)
	if err != nil {
		return err
	}

	totalIssues := len(issueNumbers)
	logger.Infof("found %d issues", totalIssues)

	opts := &github.IssueListCommentsOptions{}
	opts.ListOptions.PerPage = 100

	for i, number := range issueNumbers {
		logger.Infof("[%d/%d] getting all comments for issue: %d",
			i+1, totalIssues, number)
		logger.With(log.Fields{"number": number})

		page := 1
		for {
			logger.Infof("[page %d] fetching comments from api", page)
			logger.With(log.Fields{"page": page})

			resource, r, err := c.client.Request(
				func(c *github.Client) (interface{}, *github.Response, error) {
					return c.Issues.ListComments(
						context.TODO(), owner, repo, number, opts)
				})

			if err != nil {
				logger.Errorf(err, "api error, skipping next pages")
				break
			}

			comments := resource.([]*github.IssueComment)
			totalComments := len(comments)
			logger.Infof("found %d comments", totalComments)
			for _, comment := range comments {
				if err := c.doSync(comment); err != nil {
					logger.Errorf(err, "issue sync error, skipping comment")
				}
			}

			if r.NextPage == 0 {
				break
			}

			opts.Page = r.NextPage
			page = r.NextPage
		}
	}

	return nil
}

func (c *CommentsCommand) doSync(comment *github.IssueComment) error {
	record, err := c.store.FindOne(models.NewIssueCommentQuery().
		Where(kallax.Eq(models.Schema.IssueComment.ID, comment.GetID())),
	)

	if record == nil {
		record = models.NewIssueComment()
		record.IssueComment = *comment

		return c.store.Insert(record)
	}

	record.IssueComment = *comment
	_, err = c.store.Update(record)

	return err
}

func (c *CommentsCommand) getRepositories(db *sql.DB, excluded []string) ([]string, error) {
	repoStore := models.NewRepositoryStore(db)

	excludedRaw := make([]interface{}, len(excluded))
	for i, v := range excluded {
		excludedRaw[i] = v
	}

	reposRecors, err := repoStore.Find(models.NewRepositoryQuery().
		Where(kallax.Not(kallax.In(models.Schema.Repository.Name, excludedRaw...))))
	if err != nil {
		return nil, err
	}

	var names []string
	for reposRecors.Next() {
		repo, err := reposRecors.Get()
		if err != nil {
			return nil, err
		}

		names = append(names, *repo.Name)
	}

	return names, nil
}

func (c *CommentsCommand) getIssuesNumbersForRepo(db *sql.DB, owner string, repo string) ([]int, error) {
	prsStore := models.NewPullRequestStore(db)
	issuesStore := models.NewIssueStore(db)

	var numbers []int

	prsRecords, err := prsStore.Find(models.NewPullRequestQuery().
		Where(kallax.And(
			kallax.Eq(models.Schema.PullRequest.RepositoryOwner, owner),
			kallax.Eq(models.Schema.PullRequest.RepositoryName, repo),
		)))
	if err != nil {
		return nil, err
	}

	for prsRecords.Next() {
		pr, err := prsRecords.Get()
		if err != nil {
			return nil, err
		}

		numbers = append(numbers, *pr.Number)
	}

	issuesRecords, err := issuesStore.Find(models.NewIssueQuery().
		Where(kallax.And(
			kallax.Eq(models.Schema.Issue.RepositoryOwner, owner),
			kallax.Eq(models.Schema.Issue.RepositoryName, repo),
		)))
	if err != nil {
		return nil, err
	}

	for issuesRecords.Next() {
		issue, err := issuesRecords.Get()
		if err != nil {
			return nil, err
		}

		numbers = append(numbers, *issue.Number)
	}

	return numbers, nil
}
