<?php

declare(strict_types=1);

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks;

class SlowJob extends TestJob
{
    public function handle(): void
    {
        sleep(3);
    }
}
