package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/golang/glog"
	"github.com/urfave/cli"

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

		glog.V(3).Infof("Checking for URL: %q", req.Request.URL)

		// Check if URL exists in cache
		has, err := c.Has(req.Request.URL)
		if err != nil {
			// Log cache error
			glog.Errorf("Cache error: %v", err)
			// Notice it failed so we can retry later
			msg.Fail()
			return
		}

		// If request is not already cached we will queue it
		if !has {
			glog.V(3).Infof("Publishing to %s: %q", writeTopic, req.Request.URL)
			if err := producer.PublishJSON(writeTopic, &req); err != nil {
				// Log producer error
				glog.Errorf("Publish error: %v", err)
				// Notice it failed so we can retry later
				msg.Fail()
				return
			}
			// Save to cache to se won't schedule it again
			if err := c.Add(req.Request.URL); err != nil {
				glog.Errorf("Cache save error: %v", err)
			}
		} else {
			glog.V(3).Infof("Cached already: %q", req.Request.URL)
		}

		// Message was successfuly processed at this point
		msg.Success()
	}
}

var flags = []cli.Flag{
	cli.IntFlag{
		Name:   "verbosity",
		EnvVar: "VERBOSITY",
		Usage:  "logging verbosity",
	},
	cli.StringSliceFlag{
		Name:   "nsq-addr",
		EnvVar: "NSQ_ADDR",
	},
	cli.StringSliceFlag{
		Name:   "nsqlookup-addr",
		EnvVar: "NSQLOOKUP_ADDR",
	},
	cli.StringSliceFlag{
		Name:  "topic",
		Usage: "nsq topic in format `read_topic:write_topic`",
	},
	cli.StringFlag{
		Name:   "channel",
		EnvVar: "CHANNEL",
		Usage:  "nsq consumer channel (required)",
		Value:  "consumer",
	},
	cli.StringFlag{
		Name:   "cache-path",
		EnvVar: "CACHE_PATH",
		Usage:  "cache path on disk (required)",
		Value:  "/tmp/crawl-cache",
	},
}

func appMain(c *cli.Context) error {
	// We are setting glog to log to stderr
	flag.CommandLine.Parse([]string{"-logtostderr", fmt.Sprintf("-v=%d", c.Int("verbosity"))})

	// Open LevelDB cache
	ch, err := leveldb.Open(c.String("cache-path"))
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
	if err := connectNSQ(c); err != nil {
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

func connectNSQ(c *cli.Context) (err error) {
	if len(c.StringSlice("nsq-addr")) == 0 {
		return errors.New("at least one --nsq-addr is required")
	}

	nsqAddr := c.StringSlice("nsq-addr")[0]
	if err := producer.Connect(nsqAddr); err != nil {
		return fmt.Errorf("error connecting producer to %q: %v", nsqAddr, err)
	}

	if addrs := c.StringSlice("nsq-addr"); len(addrs) != 0 {
		for _, addr := range addrs {
			glog.V(2).Infof("Connecting to nsq %s", addr)
			if err := consumer.Connect(addr); err != nil {
				return fmt.Errorf("error connecting to nsq %q: %v", addr, err)
			}
			glog.V(2).Infof("Connected to nsq %s", addr)
		}
	}

	if addrs := c.StringSlice("nsqlookup-addr"); len(addrs) != 0 {
		for _, addr := range addrs {
			glog.V(2).Infof("Connecting to nsq lookup %s", addr)
			if err := consumer.ConnectLookupd(addr); err != nil {
				return fmt.Errorf("error connecting to nsq lookup %q: %v", addr, err)
			}
			glog.V(2).Infof("Connected to nsq lookup %s", addr)
		}
	}
	return
}

func main() {
	defer glog.Flush()

	app := cli.NewApp()
	app.Name = "crawl-cache"
	app.Usage = "crawl queue caching interceptor"
	app.Flags = flags
	app.Action = appMain

	if err := app.Run(os.Args); err != nil {
		glog.Fatal(err)
	}
}
