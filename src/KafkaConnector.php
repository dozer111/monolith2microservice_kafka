<?php

declare(strict_types=1);

namespace dozer111\Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

final class KafkaConnector implements ConnectorInterface
{

    public function connect(array $config)
    {
        $conf = new \Rdkafka\Conf();

        $conf->set('bootstrap.servers',$config['bootstrap_servers']);
        $conf->set('security.protocol',$config['security_protocol']);
        $conf->set('sasl.mechanism',$config['sasl_mechanisms']);
        $conf->set('sasl.username',$config['sasl_username']);
        $conf->set('sasl.password',$config['sasl_password']);

        $producer = new Producer($conf);

        $conf->set('group.id',$config['group_id']); // не важно
        $conf->set('auto.offset.reset','earliest');

        $consumer = new \RdKafka\KafkaConsumer($conf);
        return new KafkaQueue($producer,$consumer);
    }
}
