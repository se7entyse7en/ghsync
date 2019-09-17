package subcmd

import (
	"strings"

	"github.com/src-d/ghsync/deep"
	"github.com/src-d/ghsync/utils"

	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-log.v1"
	"gopkg.in/src-d/go-queue.v1"
	_ "gopkg.in/src-d/go-queue.v1/amqp"
	_ "gopkg.in/src-d/go-queue.v1/memory"
)

type DeepCommand struct {
	cli.Command `name:"deep" short-description:"Deep sync of GitHub data" long-description:"Deep sync of GitHub data"`

	Tokens string `long:"tokens" env:"GHSYNC_TOKENS" description:"GitHub personal access tokens comma separated" required:"true"`
	Org    string `long:"org" env:"GHSYNC_ORG" description:"Name of the GitHub organization" required:"true"`

	QueueOpt struct {
		Queue  string `long:"queue" env:"GHSYNC_QUEUE" description:"queue name. If it's not set the organization name will be used"`
		Broker string `long:"broker" env:"GHSYNC_BROKER" default:"amqp://localhost:5672" description:"broker service URI"`
	} `group:"go-queue connection options"`

	Postgres PostgresOpt `group:"PostgreSQL connection options"`
}

func (c *DeepCommand) Execute(args []string) error {
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

	broker, err := queue.NewBroker(c.QueueOpt.Broker)
	if err != nil {
		return err
	}

	qName := c.QueueOpt.Queue
	if qName == "" {
		qName = c.Org
	}
	queue, err := broker.Queue(qName)
	if err != nil {
		return err
	}

	syncer := deep.NewSyncer(db, client, queue)

	go func() {
		err := syncer.DoOrganization(c.Org)
		if err != nil {
			log.Errorf(err, "syncer.DoOrganization finished with error")
		}
	}()

	return syncer.Wait()
}
