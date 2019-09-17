package deep

import (
	"context"
	"database/sql"

	"github.com/google/go-github/github"
	"github.com/src-d/ghsync/models"

	"github.com/src-d/ghsync/utils"
	"gopkg.in/src-d/go-kallax.v1"
)

type OrganizationSyncer struct {
	s *models.OrganizationStore
	c *utils.WrapperClient
}

func NewOrganizationSyncer(db *sql.DB, c *utils.WrapperClient) *OrganizationSyncer {
	return &OrganizationSyncer{
		s: models.NewOrganizationStore(db),
		c: c,
	}
}

func (s *OrganizationSyncer) Sync(login string) error {
	resource, _, err := s.c.Request(
		func(c *github.Client) (interface{}, *github.Response, error) {
			return c.Organizations.Get(context.TODO(), login)
		})

	if err != nil {
		return err
	}

	org := resource.(*github.Organization)
	record, err := s.s.FindOne(models.NewOrganizationQuery().
		Where(kallax.Eq(models.Schema.Organization.Login, login)),
	)

	if record == nil {
		record = models.NewOrganization()
		record.Organization = *org

		return s.s.Insert(record)
	}

	record.Organization = *org
	_, err = s.s.Update(record)
	return err
}
