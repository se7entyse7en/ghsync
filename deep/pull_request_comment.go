package deep

import (
	"context"
	"database/sql"

	"github.com/src-d/ghsync/models"
	"github.com/src-d/ghsync/utils"

	"github.com/google/go-github/github"
	"gopkg.in/src-d/go-kallax.v1"
	log "gopkg.in/src-d/go-log.v1"
)

type PullRequestCommentSyncer struct {
	s *models.PullRequestCommentStore
	c *utils.WrapperClient
}

func NewPullRequestCommentSyncer(db *sql.DB, c *utils.WrapperClient) *PullRequestCommentSyncer {
	return &PullRequestCommentSyncer{
		s: models.NewPullRequestCommentStore(db),
		c: c,
	}
}

func (s *PullRequestCommentSyncer) SyncRepository(owner, repo string) error {
	return s.SyncPullRequest(owner, repo, 0)
}

func (s *PullRequestCommentSyncer) SyncPullRequest(owner, repo string, number int) error {
	opts := &github.PullRequestListCommentsOptions{}
	opts.ListOptions.PerPage = listOptionsPerPage

	logger := log.New(log.Fields{
		"type":  PullRequestCommentSyncTask,
		"owner": owner, "repo": repo, "number": number,
	})

	for {
		resource, r, err := s.c.Request(
			func(c *github.Client) (interface{}, *github.Response, error) {
				return c.PullRequests.ListComments(context.TODO(), owner, repo, number, opts)
			})

		if err != nil {
			return err
		}

		comments := resource.([]*github.PullRequestComment)

		for _, c := range comments {
			if err := s.doSync(c); err != nil {
				logger.Errorf(err, "issue sync error")
			}
		}

		if r.NextPage == 0 {
			break
		}

		opts.Page = r.NextPage
	}

	return nil
}

func (s *PullRequestCommentSyncer) Sync(owner string, repo string, commentID int64) error {
	resource, _, err := s.c.Request(
		func(c *github.Client) (interface{}, *github.Response, error) {
			return c.PullRequests.GetComment(context.TODO(), owner, repo, commentID)
		})

	if err != nil {
		return err
	}

	comment := resource.(*github.PullRequestComment)

	return s.doSync(comment)
}

func (s *PullRequestCommentSyncer) doSync(comment *github.PullRequestComment) error {
	record, err := s.s.FindOne(models.NewPullRequestCommentQuery().
		Where(kallax.And(
			kallax.Eq(models.Schema.PullRequestComment.ID, comment.GetID()),
		)),
	)

	if record == nil {
		record = models.NewPullRequestComment()
		record.PullRequestComment = *comment

		return s.s.Insert(record)
	}

	record.PullRequestComment = *comment
	_, err = s.s.Update(record)
	return err

}
