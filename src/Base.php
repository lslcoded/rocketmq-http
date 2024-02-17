<?php
/**
 * Created by PhpStorm.
 * User: lsl
 * Date: 2024/02/17
 * Time: 11:36
 */

namespace lslcoded\Rocketmq;

use MQ\Exception\AckMessageException;
use MQ\Exception\MessageNotExistException;
use MQ\Model\TopicMessage;
use MQ\MQClient;

/**
 * Class Base
 * @package Rocketmq
 */
class Base
{
    private $client;

    protected $producer;

    protected $consumer;

    public function __construct(){
        $this->client = new MQClient(
        // 设置HTTP接入域名（此处以公共云生产环境为例）
            config('mq.rocketmq.http_endpoint'),
            // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
            config('mq.rocketmq.access_key'),
            // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
            config('mq.rocketmq.secert_key'),
        );
    }

    /**
     * @param $topic
     * @param null $instanceId
     * @return $this
     */
    public function createProducer($topic,$instanceId ){
        !$instanceId && $instanceId =  config('mq.rocketmq.instance_id');
        $this->producer = $this->client->getProducer($instanceId, $topic);
        return $this;
    }

    /**
     * @param $topic
     * @param null $instanceId
     * @param null $groupId
     * @return $this
     */
    public function createTransProducer($topic,$instanceId, $groupId){
        !$instanceId && $instanceId =  config('mq.rocketmq.instance_id');
        $this->producer = $this->client->getTransProducer($instanceId, $topic ,$groupId);
        return $this;
    }

    /**
     * @param $topic
     * @param $groupId
     * @param null $instanceId
     * @param null $messageTag
     * @return $this
     */
    public function createConsumer($topic,$groupId,$instanceId,$messageTag){
        !$instanceId && $instanceId =  config('mq.rocketmq.instance_id');
        $this->consumer = $this->client->getConsumer($instanceId, $topic, $groupId,$messageTag);
        return $this;
    }
}