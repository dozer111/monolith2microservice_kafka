<?php

namespace dozer111\Kafka;

use Illuminate\Support\ServiceProvider;

class KafkaServiceProvider extends ServiceProvider
{
    public function boot()
    {
        $manager = $this->app['queue'];
        $manager->addConnector('kafka',fn() => new KafkaConnector());
    }
}
