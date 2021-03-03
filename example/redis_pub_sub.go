/*
 * @Author: gitsrc
 * @Date: 2021-03-03 17:30:34
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-03 17:34:37
 * @FilePath: /components-contrib-gitsrc/example/redis_pub_sub.go
 */

package main

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
		host:       "127.0.0.1:9001",
	}
}

func main() {
	NewRedisStreams
}
