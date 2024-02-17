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

/**
 * Class Base
 * @package Common\Repository\Aliyunmq
 */
class Base
{
    private $client;

    protected $producer;

    protected $consumer;

    private function createClient(){
        $this->client = new MQClient(
        // 设置HTTP接入域名（此处以公共云生产环境为例）
            C('MQ_HTTP_ENDPOINT'),
            // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
            C('MQ_ACCESS_KEY'),
            // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
            C('MQ_SECRET_KEY')
        );
    }

    /**
     * @param $topic
     * @param null $instanceId
     * @return $this
     */
    public function createProducer($topic,$instanceId = NULL){
        $this->createClient();
        !$instanceId && $instanceId = C('MQ_INST_ID');
        $this->producer = $this->client->getProducer($instanceId, $topic);
        return $this;
    }

    /**
     * @param $topic
     * @param $groupId
     * @param null $instanceId
     * @return $this
     */
    public function createConsumer($topic,$groupId,$instanceId = NULL){
        $this->createClient();
        !$instanceId && $instanceId = C('MQ_INST_ID');
        $this->consumer = $this->client->getConsumer($instanceId, $topic, $groupId);
        return $this;
    }
}