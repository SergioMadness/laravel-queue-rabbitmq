<?php

declare(strict_types=1);

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks;

use RuntimeException;

class RetryOnceJob extends TestJob
{
    public static int $runs = 0;

    public function handle(): void
    {
        if (++self::$runs === 1) {
            throw new RuntimeException('Retry regression test');
        }
    }
}
