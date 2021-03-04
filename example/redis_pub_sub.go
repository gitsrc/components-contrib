/*
 * @Author: gitsrc
 * @Date: 2021-03-03 17:30:34
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-04 13:43:33
 * @FilePath: /components-contrib-gitsrc/example/redis_pub_sub.go
 */

package main

import (
	"log"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	pubsub_redis "github.com/dapr/components-contrib/pubsub/redis"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	host              = "redisHost"
	password          = "redisPassword"
	consumerID        = "consumerID"
	enableTLS         = "enableTLS"
	processingTimeout = "processingTimeout"
	redeliverInterval = "redeliverInterval"
	queueDepth        = "queueDepth"
	concurrency       = "concurrency"
)

func getFakeProperties() map[string]string {
	return map[string]string{
		consumerID: "fakeConsumer",
		host:       "192.168.2.80:9004",
	}
}

func main() {
	fakeProperties := getFakeProperties()
	pubsubredis := pubsub_redis.NewRedisStreams(logger.NewLogger("dapr.contrib"))

	fakeMetaData := pubsub.Metadata{
		Properties: fakeProperties,
	}

	err := pubsubredis.Init(fakeMetaData)

	if err != nil {
		log.Fatalln(err)
	}

	fakeHandler := func(msg *pubsub.NewMessage) error {
		log.Println(string(msg.Data))
		// return fake error to skip executing redis client command
		return nil
	}

	subinfo := pubsub.SubscribeRequest{
		Topic:    "fakeTopic",
		Metadata: fakeMetaData.Properties,
	}

	err = pubsubredis.Subscribe(subinfo, fakeHandler)

	if err != nil {
		log.Fatalln(err)
	}

	for {
		time.Sleep(time.Second)
	}
}
