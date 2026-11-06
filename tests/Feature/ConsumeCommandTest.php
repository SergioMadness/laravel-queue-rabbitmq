<?php

declare(strict_types=1);

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Feature;

use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobTimedOut;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerStopReason;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Str;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\RetryOnceJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\SlowJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\TestJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\TestCase;

class ConsumeCommandTest extends TestCase
{
    private string $queue;

    protected function setUp(): void
    {
        parent::setUp();

        // The connector closes the connection when the worker stops, so the queue
        // cannot be deleted in tearDown(). RabbitMQ removes an auto-delete queue
        // itself once the consumer is gone.
        $this->queue = 'consume-'.Str::random(8);

        $this->connection()->declareQueue($this->queue, false, true);
    }

    protected function tearDown(): void
    {
        // The worker installs its signal handlers in the PHPUnit process. Restore the
        // defaults so that Ctrl+C still stops the rest of the test run.
        if (extension_loaded('pcntl')) {
            pcntl_alarm(0);

            foreach ([SIGINT, SIGTERM, SIGQUIT, SIGUSR2, SIGCONT, SIGALRM] as $signal) {
                pcntl_signal($signal, SIG_DFL);
            }
        }

        parent::tearDown();
    }

    public function testConsumeStopsAfterMaxJobs(): void
    {
        Queue::push(new TestJob, '', $this->queue);

        [$processed, $stopping] = $this->consume(['--max-jobs' => 1]);

        $this->assertSame(1, $processed);
        $this->assertSame(0, $stopping->status);

        if (enum_exists(WorkerStopReason::class)) {
            $this->assertSame(WorkerStopReason::MaxJobsExceeded, $stopping->reason);
        }

        // Laravel 13.18+ also reports the worker counters on the event.
        if (property_exists($stopping, 'jobsProcessed')) {
            $this->assertSame(1, $stopping->jobsProcessed);
            $this->assertNotNull($stopping->lastJobProcessedAt);
        }
    }

    public function testConsumeStopsWhenQueueIsEmpty(): void
    {
        [$processed, $stopping] = $this->consume(['--stop-when-empty' => true]);

        $this->assertSame(0, $processed);
        $this->assertSame(0, $stopping->status);

        if (enum_exists(WorkerStopReason::class)) {
            $this->assertSame(WorkerStopReason::QueueEmpty, $stopping->reason);
        }
    }

    public function testConsumeStopsAfterQueueRestart(): void
    {
        Event::listen(JobProcessed::class, function (): void {
            $this->artisan('queue:restart')->run();
        });
        Queue::push(new TestJob, '', $this->queue);

        [$processed, $stopping] = $this->consume([]);

        $this->assertSame(1, $processed);
        $this->assertSame(0, $stopping->status);
        $this->assertSame(WorkerStopReason::ReceivedRestartSignal, $stopping->reason);
    }

    public function testConsumeRetriesFailedJob(): void
    {
        RetryOnceJob::$runs = 0;
        $exceptions = 0;
        Event::listen(JobExceptionOccurred::class, function () use (&$exceptions): void {
            $exceptions++;
        });
        Queue::push(new RetryOnceJob, '', $this->queue);

        [$processed, $stopping] = $this->consume(['--tries' => 2, '--max-jobs' => 2]);

        $this->assertSame(2, RetryOnceJob::$runs);
        $this->assertSame(1, $exceptions);
        $this->assertSame(1, $processed);
        $this->assertSame(WorkerStopReason::MaxJobsExceeded, $stopping->reason);
    }

    public function testConsumeEnforcesJobTimeout(): void
    {
        if (! extension_loaded('pcntl') || ! property_exists(Worker::class, 'killOnTimeout')) {
            $this->markTestSkipped('Requires pcntl and a worker with configurable timeout termination.');
        }

        $timedOut = 0;
        Event::listen(JobTimedOut::class, function () use (&$timedOut): void {
            $timedOut++;
        });
        config(['queue.failed.driver' => null]);
        Queue::push(new SlowJob, '', $this->queue);
        $previous = Worker::$killOnTimeout;
        Worker::$killOnTimeout = false;

        try {
            [$processed, $stopping] = $this->consume(['--timeout' => 1, '--tries' => 1, '--max-jobs' => 1]);
        } finally {
            Worker::$killOnTimeout = $previous;
        }

        $this->assertSame(1, $timedOut);
        $this->assertSame(0, $processed);
        $this->assertSame(WorkerStopReason::MaxJobsExceeded, $stopping->reason);
    }

    public function testConsumeStopsAfterIdleLimit(): void
    {
        $start = hrtime(true);
        [, $stopping] = $this->consume(['--stop-when-empty-for' => 1]);

        $this->assertGreaterThanOrEqual(1, (hrtime(true) - $start) / 1e9);
        $this->assertSame(WorkerStopReason::QueueEmptyFor, $stopping->reason);
    }

    /**
     * Run rabbitmq:consume on the test queue and return the number of processed jobs
     * and the WorkerStopping event. --max-time stops the consumer if the stop
     * condition under test is never met, so a regression fails instead of hanging.
     *
     * @return array{0: int, 1: WorkerStopping}
     */
    private function consume(array $options): array
    {
        $processed = 0;
        $stopping = null;

        Event::listen(JobProcessed::class, function () use (&$processed): void {
            $processed++;
        });

        Event::listen(WorkerStopping::class, function (WorkerStopping $event) use (&$stopping): void {
            $stopping = $event;
        });

        $this->artisan('rabbitmq:consume', [
            '--queue' => $this->queue,
            '--sleep' => 0,
            '--max-time' => 10,
        ] + $options)->run();

        $this->assertInstanceOf(WorkerStopping::class, $stopping);

        return [$processed, $stopping];
    }
}
