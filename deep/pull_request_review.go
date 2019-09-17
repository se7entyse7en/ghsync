package deep

import (
	"context"
	"database/sql"

	"github.com/src-d/ghsync/models"
	"github.com/src-d/ghsync/utils"

	"github.com/google/go-github/github"
	"gopkg.in/src-d/go-kallax.v1"
)

type PullRequestReviewSyncer struct {
	s *models.PullRequestReviewStore
	c *utils.WrapperClient
}

func NewPullRequestReviewSyncer(db *sql.DB, c *utils.WrapperClient) *PullRequestReviewSyncer {
	return &PullRequestReviewSyncer{
		s: models.NewPullRequestReviewStore(db),
		c: c,
	}
}

func (s *PullRequestReviewSyncer) SyncPullRequest(owner, repo string, number int) error {
	opts := &github.ListOptions{}
	opts.PerPage = listOptionsPerPage

	for {
		resource, r, err := s.c.Request(
			func(c *github.Client) (interface{}, *github.Response, error) {
				return c.PullRequests.ListReviews(context.TODO(), owner, repo, number, opts)
			})

		if err != nil {
			return err
		}

		reviews := resource.([]*github.PullRequestReview)

		for _, r := range reviews {
			if err := s.doSync(r); err != nil {
				return err
			}
		}

		if r.NextPage == 0 {
			break
		}

		opts.Page = r.NextPage
	}

	return nil
}

func (s *PullRequestReviewSyncer) Sync(owner string, repo string, number int, reviewID int64) error {
	resource, _, err := s.c.Request(
		func(c *github.Client) (interface{}, *github.Response, error) {
			return c.PullRequests.GetReview(context.TODO(), owner, repo, number, reviewID)
		})

	if err != nil {
		return err
	}

	review := resource.(*github.PullRequestReview)

	return s.doSync(review)
}

func (s *PullRequestReviewSyncer) doSync(review *github.PullRequestReview) error {
	record, err := s.s.FindOne(models.NewPullRequestReviewQuery().
		Where(kallax.And(
			kallax.Eq(models.Schema.PullRequestReview.ID, review.GetID()),
		)),
	)
	if record == nil {
		record = models.NewPullRequestReview()
		record.PullRequestReview = *review

		return s.s.Insert(record)
	}

	record.PullRequestReview = *review
	_, err = s.s.Update(record)
	return err

}
