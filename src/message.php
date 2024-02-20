<?php
declare (strict_types=1);
/**
 * Created by PhpStorm.
 * User: lsl
 * Date: 2019/08/06
 * Time: 11:36
 */
namespace lslcoded\rocketmq;

use MQ\Exception\AckMessageException;
use MQ\Exception\MessageNotExistException;
use MQ\Model\TopicMessage;
use MQ\MQClient;
use Think\Exception;
use think\facade\Log;


class message extends Base
{
    /**
     * @param $topic
     * @param $instanceId
     * @param $message
     * @param $delayTimeInMillis
     * @return false
     */
    public  function publishMessage($topic, $instanceId , $message , $delayTimeInMillis = 0){
        $log = [
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
            $publishMessage->putProperty("requestId", REQUEST_ID);
            // 设置消息KEY
            $publishMessage->setMessageKey(REQUEST_ID);
            // 定时消息,精确到毫秒，当前时间往后的毫秒时间戳
            if ($delayTimeInMillis && $delayTimeInMillis>0) {
                $publishMessage->setStartDeliverTime($delayTimeInMillis);
            }
            $topicMessage = $this->producer->publishMessage($publishMessage);
            $log['info'] = 'Message has send';
            $log['topicMessage'] = $topicMessage;
            Log::info(json_encode($log));
            return $topicMessage;
        }catch (Exception $exception){
            $log['info'] = 'Message send error';
            $log['errorFile'] = $exception->getFile();
            $log['errorLine'] = $exception->getLine();
            $log['errorMessage'] = $exception->getMessage();
            $log['traceString'] = $exception->getTraceAsString();
            Log::error(json_encode($log));
            return false;
        }
    }

    /**
     * @param $receiptHandles
     * @return void
     */
    private function ackMessages($receiptHandles)
    {
        try {
            $this->consumer->ackMessage($receiptHandles);
        } catch (\Exception $e) {
            if ($e instanceof AckMessageException) {
                // 某些消息的句柄可能超时，会导致消费确认失败。
                Log::error("Ack Error, RequestId:".$e->getRequestId());
                foreach ($e->getAckMessageErrorItems() as $errorItem) {
                    $errLog['ReceiptHandle'] =  $errorItem->getReceiptHandle();
                    $errLog['ErrorCode'] =  $errorItem->getErrorCode();
                    $errLog['ErrorMsg'] =  $errorItem->getErrorMessage();
                    Log::error($errLog);
                }
            }
        }
    }

    /**
     * @param $className
     * @param $message
     * @return void
     */
    private  function executeConsumer($className, $message){
        $msg_log['publishTime'] = $message->getPublishTime();
        $msg_log['nextConsumeTime'] = $message->getNextConsumeTime();
        $msg_log['firstConsumeTime'] = $message->getFirstConsumeTime();
        $msg_log['consumedTimes'] = $message->getConsumedTimes();
        $msg_log['messageId'] = $message->getMessageId();
        $msg_log['messageBodyMD5'] = $message->getMessageBodyMD5();
        $msg_log['messageBody'] = $message->getMessageBody();
        $msg_log['messageTag'] = $message->getMessageTag();
        $msg_log['receiptHandle'] = $message->getReceiptHandle();
        $msg_log['properties'] = $message->getProperties();
        $msg_log['info'] =  'consumer Message And Start Handle';
        Log::info(json_encode($msg_log));
        (new $className())->execute($message);
    }

    /**
     * @param $topic
     * @param $goupId
     * @param null $instanceId
     */
    public function consumeMessage($topic,$goupId,$className,$instanceId = NULL){
        $this->createConsumer($topic,$goupId,$instanceId);
        // 在当前线程循环消费消息，建议是多开个几个线程并发消费消息
        while (True) {
            try {
                // 长轮询消费消息
                // 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
                $messages = $this->consumer->consumeMessage(
                    config('mq.rocketmq.num_of_messages'), // 一次最多消费3条(最多可设置为16条)
                    config('mq.rocketmq.wait_seconds')// 长轮询时间3秒（最多可设置为30秒）
                );
            }catch (\MQ\Exception\MessageResolveException $e) {
                // 当出现消息Body存在不合法字符，无法解析的时候，会抛出此异常。
                // 可以正常解析的消息列表。
                $messages = $e->getPartialResult()->getMessages();
                // 无法正常解析的消息列表。
                $failMessages = $e->getPartialResult()->getFailResolveMessages();

                $receiptHandles = array();
                foreach ($messages as $message) {
                    // 处理业务逻辑。
                    $receiptHandles[] = $message->getReceiptHandle();
                    $this->executeConsumer($className,$message);
                }
                foreach ($failMessages as $failMessage) {
                    // 处理存在不合法字符，无法解析的消息。
                    $receiptHandles[] = $failMessage->getReceiptHandle();
                    Log::error("Fail To Resolve Message. MsgID ". $failMessage->getMessageId());
                }
                $this->ackMessages($receiptHandles);
                continue;
            } catch (\Exception $e) {
                if ($e instanceof \MQ\Exception\MessageNotExistException) {
                    // 没有消息可以消费，接着轮询
                    Log::info("No message, contine long polling! RequestId:".$e->getRequestId());
                    continue;
                }
                $log['info'] = 'consume Message error';
                $log['errorFile'] = $e->getFile();
                $log['errorLine'] = $e->getLine();
                $log['errorMessage'] = $e->getMessage();
                $log['traceString'] = $e->getTraceAsString();
                Log::error(json_encode($log));
                sleep(config('mq.rocketmq.wait_seconds'));
                continue;
            }
            // 处理业务逻辑
            $receiptHandles = array();
            foreach ($messages as $message) {
                $receiptHandles[] = $message->getReceiptHandle();
                $this->executeConsumer($className,$message);
            }
            // $message->getNextConsumeTime()前若不确认消息消费成功，则消息会重复消费
            // 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
            $this->ackMessages($receiptHandles);
        }
    }
}