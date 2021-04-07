package rocketmq

import (
	"context"
	"fmt"
	hmq "github.com/aliyunmq/mq-http-go-sdk"
	mqc "github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/gogap/errors"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

type MQHttpConsumer struct {
	md         *Metadata
	client     hmq.MQClient
	contextMap sync.Map
}

func init() {
	Consumers[DefaultHttpAccessProto] = &MQHttpConsumer{}
}

// NewMQHttpConsumer
func NewMQHttpConsumer(md *Metadata) (*MQHttpConsumer, error) {
	mq := &MQHttpConsumer{md: md}
	return mq, mq.Init(md)
}

func (mq *MQHttpConsumer) Init(md *Metadata) error {
	mq.md = md
	mq.client = hmq.NewAliyunMQClientWithTimeout(md.Endpoint, md.AccessKey, md.SecretKey, "", time.Second*defaultHttpMQClientTimeoutSeconds)
	if md.ConsumerBatchSize < 1 {
		mq.md.ConsumerBatchSize = defaultConsumerNumOfMessages
	}
	return nil
}

func (mq *MQHttpConsumer) consumeMsg(ctx context.Context, mqConsumer hmq.MQConsumer, topic string, msgEntry hmq.ConsumeMessageEntry, f func(context.Context, ...*primitive.MessageExt) (mqc.ConsumeResult, error)) error {
	msg := &primitive.MessageExt{}
	msg.MsgId = msgEntry.MessageId
	msg.BornTimestamp = msgEntry.PublishTime
	msg.Topic = topic
	msg.Body = []byte(msgEntry.MessageBody)
	msg.WithProperties(msgEntry.Properties)
	status, err := f(ctx, msg)
	if err != nil {
		return fmt.Errorf("consume message failed. topic:%s MessageID:%s %w", topic, msgEntry.MessageId, err)
	}

	if status != mqc.ConsumeSuccess {
		return fmt.Errorf("status not success,topic:%s MessageID:%s", topic, msgEntry.MessageId)
	}

	var handles []string
	handles = append(handles, msgEntry.ReceiptHandle)
	ackErr := mqConsumer.AckMessage(handles)
	if ackErr != nil {
		for _, errAckItem := range ackErr.(errors.ErrCode).Context()["Detail"].([]hmq.ErrAckItem) {
			log.Printf("[rocketmq-http] topic: %s, errorHandle:%s, errorCode:%s, errorMsg:%s",
				topic, errAckItem.ErrorHandle, errAckItem.ErrorCode, errAckItem.ErrorMsg)
		}
		return fmt.Errorf("ack message failed. handle:%s", msgEntry.ReceiptHandle)
	}
	return nil
}

func (mq *MQHttpConsumer) consumeInner(quitCh chan int, topic string, mqConsumer hmq.MQConsumer, f func(context.Context, ...*primitive.MessageExt) (mqc.ConsumeResult, error)) {
	done := false
	for !done {
		respChan := make(chan hmq.ConsumeMessageResponse)
		errChan := make(chan error)
		go func() {
			mqConsumer.ConsumeMessage(respChan, errChan, int32(mq.md.ConsumerBatchSize), defaultHttpConsumerWaitSeconds)
		}()

		var err error
		select {
		case <-quitCh:
			quitCh <- 1
			done = true
		case resp := <-respChan:
			if len(resp.Messages) > 1 {
				wg := sync.WaitGroup{}
				wg.Add(len(resp.Messages))
				for _, v := range resp.Messages {
					msg := v
					go func() {
						defer wg.Done()
						err = mq.consumeMsg(context.TODO(), mqConsumer, topic, msg, f)
					}()
				}
				wg.Wait()
			} else if len(resp.Messages) == 1 {
				err = mq.consumeMsg(context.TODO(), mqConsumer, topic, resp.Messages[0], f)
			}
		case err := <-errChan:
			if !strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
				log.Println(err)
				time.Sleep(time.Duration(3) * time.Second)
			} else {
				//log.Println("[rocketmq-http] No new message, continue!")
			}
		case <-time.After(defaultHttpConsumerTimeoutSeconds * time.Second):
			log.Printf("[rocketmq-http] topic: %s, timeout of consumer message\n", topic)
		}
		if err != nil {
			log.Println(err)
		}
	}
}

// Start the PullConsumer for consuming message
func (mq *MQHttpConsumer) Start() error {
	return nil
}

// Shutdown the PullConsumer
func (mq *MQHttpConsumer) Shutdown() error {
	mq.contextMap.Range(func(key, value interface{}) bool {
		if err := mq.Unsubscribe(key.(string)); err != nil {
			return false
		}
		return true
	})
	return nil
}

// Subscribe a topic for consuming
func (mq *MQHttpConsumer) Subscribe(topic string, selector mqc.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (mqc.ConsumeResult, error)) error {
	mqConsumer := mq.client.GetConsumer(mq.md.InstanceId, topic, mq.md.ConsumerGroup, selector.Expression)
	quitCh := make(chan int)
	mq.contextMap.Store(topic, quitCh)
	go mq.consumeInner(quitCh, topic, mqConsumer, f)
	return nil
}

// Unsubscribe a topic
func (mq *MQHttpConsumer) Unsubscribe(topic string) error {
	v, ok := mq.contextMap.LoadAndDelete(topic)
	if ok {
		quitCh := v.(chan int)
		quitCh <- 1
		<-quitCh
		close(quitCh)
	}
	return nil
}
