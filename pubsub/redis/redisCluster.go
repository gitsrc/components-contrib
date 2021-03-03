// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"

	"github.com/dapr/components-contrib/pubsub"
)

func (r *redisStreams) ClusterInit(metadata pubsub.Metadata) error {
	m, err := parseRedisMetadata(metadata)
	if err != nil {
		return err
	}
	r.metadata = m

	options := &redis.ClusterOptions{
		Addrs:           strings.Split(m.host, ","),
		Password:        m.password,
		MaxRetries:      3,
		MaxRetryBackoff: time.Second * 2,
	}

	/* #nosec */
	if r.metadata.enableTLS {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: r.metadata.enableTLS,
		}
	}

	clusterClient := redis.NewClusterClient(options)
	if _, err = clusterClient.Ping().Result(); err != nil {
		return fmt.Errorf("redis streams: error connecting to redis at %s: %s", m.host, err)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.queue = make(chan redisMessageWrapper, int(r.metadata.queueDepth))
	r.clusterClient = clusterClient

	for i := uint(0); i < r.metadata.concurrency; i++ {
		go r.worker()
	}

	return nil
}

func (r *redisStreams) ClusterPublish(req *pubsub.PublishRequest) error {
	_, err := r.clusterClient.XAdd(&redis.XAddArgs{
		Stream: req.Topic,
		Values: map[string]interface{}{"data": req.Data},
	}).Result()
	if err != nil {
		return fmt.Errorf("redis streams: error from publish: %s", err)
	}

	return nil
}

func (r *redisStreams) ClusterSubscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	err := r.clusterClient.XGroupCreateMkStream(req.Topic, r.metadata.consumerID, "0").Err()
	// Ignore BUSYGROUP errors
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		r.logger.Errorf("redis streams: %s", err)

		return err
	}

	go r.clusterpollNewMessagesLoop(req.Topic, handler)
	go r.clusterreclaimPendingMessagesLoop(req.Topic, handler)

	return nil
}

// pollMessagesLoop calls `XReadGroup` for new messages and funnels them to the message channel
// by calling `enqueueMessages`.
func (r *redisStreams) clusterpollNewMessagesLoop(stream string, handler func(msg *pubsub.NewMessage) error) {
	for {
		// Read messages
		streams, err := r.clusterClient.XReadGroup(&redis.XReadGroupArgs{
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			Streams:  []string{stream, ">"},
			Count:    int64(r.metadata.queueDepth),
			Block:    0,
		}).Result()
		if err != nil {
			r.logger.Errorf("redis streams: error reading from stream %s: %s", stream, err)

			continue
		}

		// Enqueue messages for the returned streams
		for _, s := range streams {
			r.enqueueMessages(s.Stream, handler, s.Messages)
		}

		// Return on cancelation
		if r.ctx.Err() != nil {
			return
		}
	}
}

// reclaimPendingMessagesLoop periodically reclaims pending messages
// based on the `redeliverInterval` setting.
func (r *redisStreams) clusterreclaimPendingMessagesLoop(stream string, handler func(msg *pubsub.NewMessage) error) {
	// Having a `processingTimeout` or `redeliverInterval` means that
	// redelivery is disabled so we just return out of the goroutine.
	if r.metadata.processingTimeout == 0 || r.metadata.redeliverInterval == 0 {
		return
	}

	// Do an initial reclaim call
	r.clusterreclaimPendingMessages(stream, handler)

	reclaimTicker := time.NewTicker(r.metadata.redeliverInterval)

	for {
		select {
		case <-r.ctx.Done():
			return

		case <-reclaimTicker.C:
			r.clusterreclaimPendingMessages(stream, handler)
		}
	}
}

// reclaimPendingMessages handles reclaiming messages that previously failed to process and
// funneling them to the message channel by calling `enqueueMessages`.
func (r *redisStreams) clusterreclaimPendingMessages(stream string, handler func(msg *pubsub.NewMessage) error) {
	for {
		// Retrieve pending messages for this stream and consumer
		pendingResult, err := r.clusterClient.XPendingExt(&redis.XPendingExtArgs{
			Stream: stream,
			Group:  r.metadata.consumerID,
			Start:  "-",
			End:    "+",
			Count:  int64(r.metadata.queueDepth),
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error retrieving pending Redis messages: %v", err)

			break
		}

		// Filter out messages that have not timed out yet
		msgIDs := make([]string, 0, len(pendingResult))
		for _, msg := range pendingResult {
			if msg.Idle >= r.metadata.processingTimeout {
				msgIDs = append(msgIDs, msg.ID)
			}
		}

		// Nothing to claim
		if len(msgIDs) == 0 {
			break
		}

		// Attempt to claim the messages for the filtered IDs
		claimResult, err := r.clusterClient.XClaim(&redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: msgIDs,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error claiming pending Redis messages: %v", err)

			break
		}

		// Enqueue claimed messages
		r.enqueueMessages(stream, handler, claimResult)

		// If the Redis nil error is returned, it means somes message in the pending
		// state no longer exist. We need to acknowledge these messages to
		// remove them from the pending list.
		if errors.Is(err, redis.Nil) {
			// Build a set of message IDs that were not returned
			// that potentially no longer exist.
			expectedMsgIDs := make(map[string]struct{}, len(msgIDs))
			for _, id := range msgIDs {
				expectedMsgIDs[id] = struct{}{}
			}
			for _, claimed := range claimResult {
				delete(expectedMsgIDs, claimed.ID)
			}

			r.clusterremoveMessagesThatNoLongerExistFromPending(stream, expectedMsgIDs, handler)
		}
	}
}

// removeMessagesThatNoLongerExistFromPending attempts to claim messages individually so that messages in the pending list
// that no longer exist can be removed from the pending list. This is done by calling `XACK`.
func (r *redisStreams) clusterremoveMessagesThatNoLongerExistFromPending(stream string, messageIDs map[string]struct{}, handler func(msg *pubsub.NewMessage) error) {
	// Check each message ID individually.
	for pendingID := range messageIDs {
		claimResultSingleMsg, err := r.clusterClient.XClaim(&redis.XClaimArgs{
			Stream:   stream,
			Group:    r.metadata.consumerID,
			Consumer: r.metadata.consumerID,
			MinIdle:  r.metadata.processingTimeout,
			Messages: []string{pendingID},
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Errorf("error claiming pending Redis message %s: %v", pendingID, err)

			continue
		}

		// Ack the message to remove it from the pending list.
		if errors.Is(err, redis.Nil) {
			if err = r.clusterClient.XAck(stream, r.metadata.consumerID, pendingID).Err(); err != nil {
				r.logger.Errorf("error acknowledging Redis message %s after failed claim for %s: %v", pendingID, stream, err)
			}
		} else {
			// This should not happen but if it does the message should be processed.
			r.enqueueMessages(stream, handler, claimResultSingleMsg)
		}
	}
}

func (r *redisStreams) ClusterClose() error {
	r.cancel()

	return r.clusterClient.Close()
}
