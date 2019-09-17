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

type UserSyncer struct {
	s *models.UserStore
	c *utils.WrapperClient
}

func NewUserSyncer(db *sql.DB, c *utils.WrapperClient) *UserSyncer {
	return &UserSyncer{
		s: models.NewUserStore(db),
		c: c,
	}
}

func (s *UserSyncer) QueueOrganization(q queue.Queue, org string) error {
	opts := &github.ListMembersOptions{}
	opts.ListOptions.PerPage = listOptionsPerPage

	logger := log.New(log.Fields{"type": UserSyncTask, "owner": org})
	logger.Infof("starting to publish queue jobs")

	for {
		resource, r, err := s.c.Request(
			func(c *github.Client) (interface{}, *github.Response, error) {
				return c.Organizations.ListMembers(context.TODO(), org, opts)
			})

		if err != nil {
			return err
		}

		users := resource.([]*github.User)
		for _, u := range users {
			j, err := NewUserSyncJob(u.GetLogin())
			if err != nil {
				return err
			}

			logger.With(log.Fields{"user": u.GetLogin()}).Debugf("queue request")
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

func (s *UserSyncer) Sync(login string) error {
	resource, _, err := s.c.Request(
		func(c *github.Client) (interface{}, *github.Response, error) {
			return c.Users.Get(context.TODO(), login)
		})

	if err != nil {
		return err
	}

	user := resource.(*github.User)

	record, err := s.s.FindOne(models.NewUserQuery().
		Where(kallax.And(
			kallax.Eq(models.Schema.User.ID, user.GetID()),
		)),
	)

	if record == nil {
		record = models.NewUser()
		record.User = *user

		return s.s.Insert(record)
	}

	record.User = *user
	_, err = s.s.Update(record)
	return err

}
