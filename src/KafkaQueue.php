<?php

declare(strict_types=1);

namespace dozer111\Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use RdKafka\KafkaConsumer;
use RdKafka\Producer;

final class KafkaQueue extends Queue implements QueueContract
{
    private Producer $producer;
    private KafkaConsumer $consumer;

    public function __construct(Producer $producer, KafkaConsumer $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        $producer = $this->producer;
        $topic = $producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $producer->flush(50);
        if ($producer->getOutQLen()) {
            sleep(2);
            $producer->poll(50);
        }
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    public function pop($queue = null)
    {
        try {
            $consumer = $this->consumer;
            $consumer->subscribe([$queue ?? env('KAFKA_QUEUE')]);
            $message = $consumer->consume(5 * 1000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No messages. Waiting ...\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        } catch (\Throwable $e) {
            var_dump([
                'class' => get_class($e),
                'msg' => $e->getMessage(),
            ]);
        }
    }
}
