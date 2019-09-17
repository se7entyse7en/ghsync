package deep

import (
	"context"
	"database/sql"

	"github.com/src-d/ghsync/models"
	"github.com/src-d/ghsync/utils"

	"github.com/google/go-github/github"
	"gopkg.in/src-d/go-kallax.v1"
	"gopkg.in/src-d/go-log.v1"
	"gopkg.in/src-d/go-queue.v1"
)

type RepositorySyncer struct {
	s *models.RepositoryStore
	c *utils.WrapperClient
}

func NewRepositorySyncer(db *sql.DB, c *utils.WrapperClient) *RepositorySyncer {
	return &RepositorySyncer{
		s: models.NewRepositoryStore(db),
		c: c,
	}
}

func (s *RepositorySyncer) QueueOrganization(q queue.Queue, owner string) error {
	opts := &github.RepositoryListByOrgOptions{}
	opts.ListOptions.PerPage = listOptionsPerPage

	logger := log.New(log.Fields{"type": RepositorySyncTask, "owner": owner})
	logger.Infof("starting to publish queue jobs")

	for {
		resource, r, err := s.c.Request(
			func(c *github.Client) (interface{}, *github.Response, error) {
				return c.Repositories.ListByOrg(context.TODO(), owner, opts)
			})

		if err != nil {
			return err
		}

		repositories := resource.([]*github.Repository)

		for _, r := range repositories {
			j, err := NewRepositorySyncJob(owner, r.GetName())
			if err != nil {
				return err
			}

			logger.With(log.Fields{"repo": r.GetName()}).Debugf("queue request")
			if err := q.Publish(j); err != nil {
				return err
			}
		}

		if r.NextPage == 0 {
			break
		}

		opts.Page = r.NextPage
	}

	logger.Infof("finished to publish queue jobs")

	return nil
}

func (s *RepositorySyncer) Sync(owner, name string) error {
	resource, _, err := s.c.Request(
		func(c *github.Client) (interface{}, *github.Response, error) {
			return c.Repositories.Get(context.TODO(), owner, name)
		})

	if err != nil {
		return err
	}

	repository := resource.(*github.Repository)

	record, err := s.s.FindOne(models.NewRepositoryQuery().
		Where(kallax.Eq(models.Schema.Repository.ID, repository.GetID())),
	)

	if record == nil {
		record = models.NewRepository()
		record.Repository = *repository

		return s.s.Insert(record)
	}

	record.Repository = *repository
	_, err = s.s.Update(record)
	return err

}
