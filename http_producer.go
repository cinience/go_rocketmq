package rocketmq

import (
	"context"
	"errors"
	hmq "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"time"
)

type MQHttpProducer struct {
	md     *Metadata
	client hmq.MQClient
}

func init() {
	Producers[DefaultHttpAccessProto] = &MQHttpProducer{}
}

// NewMQHttpProducer
func NewMQHttpProducer(md *Metadata) (*MQHttpProducer, error) {
	mq := &MQHttpProducer{md: md}
	return mq, mq.Init(md)
}

func (mq *MQHttpProducer) Init(md *Metadata) error {
	mq.md = md
	mq.client = hmq.NewAliyunMQClientWithTimeout(md.Endpoint, md.AccessKey, md.SecretKey, "", time.Second*defaultHttpMQClientTimeoutSeconds)
	return nil
}

// Start the Producer
func (mq *MQHttpProducer) Start() error {
	return nil
}

// Shutdown the Producer
func (mq *MQHttpProducer) Shutdown() error {
	return nil
}

// Send sync message, support single msg just now
func (mq *MQHttpProducer) SendSync(ctx context.Context, msgs ...*primitive.Message) (*primitive.SendResult, error) {
	for _, m := range msgs {
		mqProducer := mq.client.GetProducer(mq.md.InstanceId, m.Topic)
		msg := hmq.PublishMessageRequest{
			MessageBody: string(m.Body),
			MessageKey:  m.GetKeys(),
			MessageTag:  m.GetTags(),
			Properties:  m.GetProperties(),
		}
		_, err := mqProducer.PublishMessage(msg)
		if err != nil {
			return nil, err
		}
	}
	rst := primitive.NewSendResult()
	rst.Status = primitive.SendOK
	rst.MessageQueue = &primitive.MessageQueue{}
	return rst, nil
}

// Send async message, unimplemented
func (mq *MQHttpProducer) SendAsync(ctx context.Context, f func(ctx context.Context, result *primitive.SendResult, err error), msg ...*primitive.Message) error {
	return errors.New("unimplemented method")
}

// Send oneway message, unimplemented
func (mq *MQHttpProducer) SendOneWay(ctx context.Context, msg ...*primitive.Message) error {
	return errors.New("unimplemented method")
}
