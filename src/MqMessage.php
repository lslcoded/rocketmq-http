<?php
/**
 * Created by PhpStorm.
 * User: lsl
 * Date: 2019/08/06
 * Time: 11:36
 */

namespace Rocketmq;

use MQ\Exception\AckMessageException;
use MQ\Exception\MessageNotExistException;
use MQ\Model\TopicMessage;
use MQ\MQClient;
use Think\Exception;

class MqMessage extends Base
{
    /**
     * @param $topic
     * @param null $instanceId
     * @param $message
     * @param int $delayTimeInMillis
     */
    public function sendMessage($topic,$instanceId = NULL,$message,$delayTimeInMillis = 0){
        $data = [
            'topic' => $topic,
            'message' => $message,
            'delayTimeInMillis' => $delayTimeInMillis,
            'requestId' => REQUEST_ID
        ];

        try{
            $this->createProducer($topic,$instanceId);
            // 消息内容
            $publishMessage = new TopicMessage($message);
            // 设置属性
            $publishMessage->putProperty("RequestId", REQUEST_ID);
            // 设置消息KEY
            $publishMessage->setMessageKey(REQUEST_ID);
            if ($delayTimeInMillis>0) {
                // 定时消息,精确到毫秒，当前时间往后的毫秒时间戳
                $publishMessage->setStartDeliverTime($delayTimeInMillis);
            }

            $this->producer->publishMessage($publishMessage);

            write_info_log(
                self::DEBUG_CHANNEL,
                __FILE__,
                __LINE__,
                'Message has send',
                $data
            );
        }catch (Exception $exception){

            write_err_log(
                self::DEBUG_CHANNEL,
                $exception->getFile(),
                $exception->getLine(),
                $exception->getMessage(),
                $data
            );
        }
    }

    /**
     * @param $topic
     * @param $goupId
     * @param null $instanceId
     */
    public function consumeMessage($topic,$goupId,$instanceId = NULL){
        $this->createConsumer($topic,$goupId,$instanceId);
        // 在当前线程循环消费消息，建议是多开个几个线程并发消费消息
        while (True) {
            try {
                // 长轮询消费消息
                // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
                $messages = $this->consumer->consumeMessage(
                    3, // 一次最多消费3条(最多可设置为16条)
                    3 // 长轮询时间3秒（最多可设置为30秒）
                );
            } catch (\Exception $e) {
                if ($e instanceof \MQ\Exception\MessageNotExistException) {
                    // 没有消息可以消费，接着轮询
                    printf("No message, contine long polling!RequestId:%s\n", $e->getRequestId());
                    continue;
                }
                print_r($e->getMessage() . "\n");
                sleep(3);
                continue;
            }
            print "consume finish, messages:\n";
            // 处理业务逻辑
            $receiptHandles = array();
            foreach ($messages as $message) {
                $receiptHandles[] = $message->getReceiptHandle();
                printf("MessageID:%s TAG:%s BODY:%s \nPublishTime:%d, FirstConsumeTime:%d, \nConsumedTimes:%d, NextConsumeTime:%d,MessageKey:%s\n",
                    $message->getMessageId(), $message->getMessageTag(), $message->getMessageBody(),
                    $message->getPublishTime(), $message->getFirstConsumeTime(), $message->getConsumedTimes(), $message->getNextConsumeTime(),
                    $message->getMessageKey());
                print_r($message->getProperties());
                $data = json_decode($message->getMessageBody(),true);
                print "业务逻辑处理\n";
            }
            // $message->getNextConsumeTime()前若不确认消息消费成功，则消息会重复消费
            // 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
            print_r($receiptHandles);
            try {
                $this->consumer->ackMessage($receiptHandles);
            } catch (\Exception $e) {
                if ($e instanceof \MQ\Exception\AckMessageException) {
                    // 某些消息的句柄可能超时了会导致确认不成功
                    printf("Ack Error, RequestId:%s\n", $e->getRequestId());
                    foreach ($e->getAckMessageErrorItems() as $errorItem) {
                        printf("\tReceiptHandle:%s, ErrorCode:%s, ErrorMsg:%s\n", $errorItem->getReceiptHandle(), $errorItem->getErrorCode(), $errorItem->getErrorCode());
                    }
                }
            }
            print "ack finish\n";
        }
    }
}