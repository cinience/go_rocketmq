// +build linux darwin,amd64,cgo
// +build rocket_cgo

package rocketmq

import (
	"context"
	"errors"
	cmq "github.com/apache/rocketmq-client-go/core"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type CRocketMQProducer struct {
	md     *Metadata
	client cmq.Producer
}

func init() {
	Producers[TCPCGOProto] = &CRocketMQProducer{}
}

// NewRocketMQProducer
func NewRocketMQProducer(md *Metadata) (*CRocketMQProducer, error) {
	mq := &CRocketMQProducer{md: md}
	return mq, mq.Init(md)
}

func (mq *CRocketMQProducer) Init(md *Metadata) error {
	mq.md = md
	config := &cmq.ProducerConfig{
		ClientConfig: cmq.ClientConfig{
			GroupID:      md.ConsumerGroup,
			NameServer:   md.NameServer,
			InstanceName: md.InstanceId,
			Credentials: &cmq.SessionCredentials{
				AccessKey: md.AccessKey,
				SecretKey: md.SecretKey,
				Channel:   defaultRocketMQPublicChannel,
			},
		},
		ProducerModel: cmq.CommonProducer,
	}
	if md.NameServer == defaultRocketMQChannel || (len(md.NameServer) == 0 && len(md.Endpoint) == 0) {
		config.Credentials.Channel = defaultRocketMQChannel
	}
	var err error
	if mq.client, err = cmq.NewProducer(config); err != nil {
		return err
	}
	return mq.Start()

}

// Start the Producer
func (mq *CRocketMQProducer) Start() error {
	return mq.client.Start()
}

// Shutdown the Producer
func (mq *CRocketMQProducer) Shutdown() error {
	return mq.client.Shutdown()
}

// Send sync message, support single msg just now
func (mq *CRocketMQProducer) SendSync(ctx context.Context, msgs ...*primitive.Message) (*primitive.SendResult, error) {
	for _, m := range msgs {
		msg := &cmq.Message{Topic: m.Topic, Tags: m.GetTags(), Body: string(m.Body), Property: m.GetProperties()}
		resp, err := mq.client.SendMessageSync(msg)

		if err != nil {
			return nil, err
		}
		rst := primitive.NewSendResult()
		rst.MsgID = resp.MsgId
		rst.Status = primitive.SendStatus(resp.Status)
		return rst, nil
	}

	return nil, nil
}

// Send async message, unimplemented
func (mq *CRocketMQProducer) SendAsync(ctx context.Context, f func(ctx context.Context, result *primitive.SendResult, err error), msg ...*primitive.Message) error {
	return errors.New("unimplemented method")
}

// Send oneway message, unimplemented
func (mq *CRocketMQProducer) SendOneWay(ctx context.Context, msg ...*primitive.Message) error {
	return errors.New("unimplemented method")
}
