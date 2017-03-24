package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	"gopkg.in/urfave/cli.v2"

	clinsq "github.com/crackcomm/cli-nsq"
	"github.com/crackcomm/crawl-cache/cache"
	"github.com/crackcomm/crawl-cache/cache/leveldb"
	"github.com/crackcomm/crawl/nsq/nsqcrawl"
	"github.com/crackcomm/nsqueue/consumer"
	"github.com/crackcomm/nsqueue/producer"
)

type topic struct{ read, write string }

func topicFromString(t string) topic {
	s := strings.Split(t, ":")
	return topic{read: s[0], write: s[1]}
}

func cleanURLPrefixes(u string) string {
	for _, prefix := range []string{
		"http://",
		"https://",
		"www.",
	} {
		u = strings.TrimPrefix(u, prefix)
	}
	return u
}

func cacheHandler(c cache.Cache, writeTopic string) consumer.Handler {
	return func(msg *consumer.Message) {
		var req nsqcrawl.Request

		if err := json.Unmarshal(msg.Body, &req); err != nil {
			// Log error
			glog.Errorf("Unmarshal error: %v", err)
			// Give up this message…
			msg.GiveUp()
			return
		}

		uri := cleanURLPrefixes(req.Request.URL)
		glog.V(3).Infof("Checking for URL: %q", uri)

		// Check if URL exists in cache
		has, err := c.Has(uri)
		if err != nil {
			// Log cache error
			glog.Errorf("Cache error: %v", err)
			// Notice it failed so we can retry later
			msg.Fail()
			return
		}

		// If request is not already cached we will queue it
		if !has {
			glog.V(3).Infof("Publishing to %s: %q", writeTopic, uri)
			if err := producer.PublishJSON(writeTopic, &req); err != nil {
				// Log producer error
				glog.Errorf("Publish error: %v", err)
				// Notice it failed so we can retry later
				msg.Fail()
				return
			}
			// Save to cache to se won't schedule it again
			if err := c.Add(uri); err != nil {
				glog.Errorf("Cache save error: %v", err)
			}
		} else {
			glog.V(3).Infof("Cached already: %q", uri)
		}

		// Message was successfuly processed at this point
		msg.Success()
	}
}

var flags = []cli.Flag{
	&cli.StringFlag{
		Name:   "cache-path",
		EnvVars: []string{"CACHE_PATH"},
		Usage:  "cache path on disk (required)",
		Value:  "/tmp/crawl-cache",
	},
	&cli.DurationFlag{
		Name:   "cache-ttl",
		EnvVars: []string{"CACHE_ttl"},
		Usage:  "cache ttl",
		Value:  time.Hour * 24 * 30,
	},
	clinsq.AddrFlag,
	clinsq.LookupAddrFlag,
	clinsq.TopicFlag,
	clinsq.ChannelFlag,
	&cli.IntFlag{
		Name:   "verbosity",
		EnvVars: []string{"VERBOSITY"},
		Usage:  "logging verbosity",
	},
}

func appMain(c *cli.Context) error {
	// We are setting glog to log to stderr
	flag.CommandLine.Parse([]string{"-logtostderr", fmt.Sprintf("-v=%d", c.Int("verbosity"))})

	// Open LevelDB cache
	ch, err := leveldb.Open(c.String("cache-path"), c.Duration("cache-ttl"))
	if err != nil {
		return err
	}

	// Close the disk cache at the end
	defer func() {
		if err := ch.Close(); err != nil {
			glog.Errorf("Cache close error: %v", err)
		}
	}()

	// Get topics to cache
	var topics []topic
	for _, t := range c.StringSlice("topic") {
		topics = append(topics, topicFromString(t))
	}

	// NSQ consumer channel
	channel := c.String("channel")

	// Register consumers for all topics
	for _, t := range topics {
		glog.Infof("Registering consumer for topic %s:%s", t.read, t.write)
		consumer.Register(t.read, channel, 1, cacheHandler(ch, t.write))
	}

	// Connect to NSQ as consumer and producer
	if err := clinsq.Connect(c); err != nil {
		return err
	}

	// Defer graceful close of consumer and producer
	defer func() {
		consumer.Stop()
		producer.Stop()
	}()

	// Start consuming from NSQ
	glog.Infof("Starting a consumer for %d topics", len(topics))
	consumer.Start(false)
	return nil
}

func main() {
	defer glog.Flush()

	app := (&cli.App{})
	app.Name = "crawl-cache"
	app.Usage = "crawl queue caching interceptor"
	app.Flags = flags
	app.Action = appMain

	if err := app.Run(os.Args); err != nil {
		glog.Fatal(err)
	}
}
