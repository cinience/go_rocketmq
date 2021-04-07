package rocketmq

import (
	"context"
	"errors"
	"github.com/apache/rocketmq-client-go/v2"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"strings"
)

/**
 *  https://help.aliyun.com/document_detail/114479.html
 */
type NativeRocketMQConsumer struct {
	md     *Metadata
	client rocketmq.PushConsumer
}

func init() {
	Consumers[TCPNativeProto] = &NativeRocketMQConsumer{}
}

func (mq *NativeRocketMQConsumer) Init(md *Metadata) error {
	var opts []mqc.Option
	mq.md = md
	if len(md.NameServer) > 0 {
		opts = append(opts, mqc.WithNameServer(splitAndTrim(md.NameServer)))
	} else {
		return errors.New("get nameserver failed")
	}

	if len(md.NameServerDomain) > 0 {
		opts = append(opts, mqc.WithNameServerDomain(md.NameServerDomain))
	}

	if md.Retries > 0 {
		opts = append(opts, mqc.WithRetry(md.Retries))
	}

	if len(md.ConsumerGroup) > 0 {
		opts = append(opts, mqc.WithGroupName(md.ConsumerGroup))
	}

	if len(md.AccessKey) > 0 {
		var credentials = primitive.Credentials{
			AccessKey: md.AccessKey,
			SecretKey: md.SecretKey,
		}
		opts = append(opts, mqc.WithCredentials(credentials))
	}

	opts = append(opts, mqc.WithNamespace(md.InstanceId))

	var err error
	mq.client, err = rocketmq.NewPushConsumer(opts...)
	return err
}

// Start the PullConsumer for consuming message
func (mq *NativeRocketMQConsumer) Start() error {
	return mq.client.Start()
}

// Shutdown the PullConsumer
func (mq *NativeRocketMQConsumer) Shutdown() error {
	return mq.client.Shutdown()
}

// Subscribe a topic for consuming
func (mq *NativeRocketMQConsumer) Subscribe(topic string, selector mqc.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (mqc.ConsumeResult, error)) error {
	return mq.client.Subscribe(topic, selector, f)
}

// Unsubscribe a topic
func (mq *NativeRocketMQConsumer) Unsubscribe(topic string) error {
	return mq.client.Unsubscribe(topic)
}

func splitAndTrim(s string) []string {
	arr := strings.Split(s, ",")
	for i, e := range arr {
		arr[i] = strings.TrimSpace(e)
	}
	return arr
}
