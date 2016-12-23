<?php

namespace Vsmoraes\Kinesis;

use Aws\Kinesis\KinesisClient;
use Vsmoraes\Kinesis\Checkpoint\Checkpoint;

class Manager
{
    const DEFAULT_TIMEMOUT = 1000;
    const DAEMONIZED = -1;
    const DEFAULT_SLEEP = 3.0;
    const SHARD_ID = 'shardId-000000000000';
    const TIMEOUT = 5.0;

    /**
     * @var KinesisClient
     */
    private $kinesisClient;

    /**
     * @var Checkpoint
     */
    private $checkpoint;

    /**
     * @var int
     */
    private $limit;

    /**
     * @var int
     */
    private $timeout;

    /**
     * @var float
     */
    protected $sleepDuration;

    public function __construct(
        KinesisClient $kinesisClient,
        Checkpoint $checkpoint,
        int $limit = self::DEFAULT_TIMEMOUT,
        float $timeout = self::TIMEOUT,
        float $sleepDuration = self::DEFAULT_SLEEP
    ) {
        $this->kinesisClient = $kinesisClient;
        $this->checkpoint = $checkpoint;
        $this->limit = $limit;
        $this->timeout = $timeout;
        $this->sleepDuration = $sleepDuration;
    }

    /**
     * @param string $streamName
     *
     * @return \Generator
     */
    public function records(string $streamName)
    {
        $shardIterator = $this->firstShardIterator($streamName);
        $lastSequenceNumber = null;
        $startTime = microtime(true);

        while ($this->loopConfig($startTime)) {
            $recordResponse = $this->kinesisClient()->getRecords([
                'ShardIterator' => $shardIterator,
                'Limit' => $this->limit(),
            ]);

            foreach ($recordResponse->get('Records') as $record) {
                $lastSequenceNumber = $record['SequenceNumber'];

                yield json_decode($record['Data'], true);
            }

            $shardIterator = $recordResponse->get('NextShardIterator');

            if (! is_null($lastSequenceNumber)) {
                $this->checkpoint->checkpoint($streamName, $lastSequenceNumber);
            }

            if ($this->isDaemonized()) {
                sleep($this->sleep());
            }
        }
    }

    /**
     * @return int
     */
    public function limit(): int
    {
        return $this->limit;
    }

    /**
     * @param float|null $timeout
     *
     * @return float
     */
    public function timeout(float $timeout = null): float
    {
        if (! is_null($timeout)) {
            $this->timeout = $timeout;
        }

        return $this->timeout;
    }

    /**
     * @param float|null $sleepDuration sleep duration in seconds
     *
     * @return float
     */
    public function sleep(float $sleepDuration = null): float
    {
        if (! is_null($sleepDuration)) {
            $this->sleepDuration = $sleepDuration;
        }

        return $this->sleepDuration;
    }

    /**
     * @return KinesisClient
     */
    public function kinesisClient(): KinesisClient
    {
        return $this->kinesisClient;
    }

    /**
     * @return bool
     */
    public function isDaemonized()
    {
        return $this->timeout() == static::DAEMONIZED;
    }

    /**
     * @param string $streamName
     *
     * @return string
     */
    protected function firstShardIterator(string $streamName): string
    {
        try {
            $checkpoint = $this->checkpoint->shardIteratorParams($streamName, self::SHARD_ID);

            $result = $this->kinesisClient()->getShardIterator($checkpoint);
        } catch (\Exception $exception) {
            $result = $this->kinesisClient()->getShardIterator([
                'StreamName' => $streamName,
                'ShardId' => self::SHARD_ID,
                'ShardIteratorType' => 'TRIM_HORIZON',
            ]);
        }

        return $result->get('ShardIterator');
    }

    /**
     * @param $startTime
     *
     * @return bool
     */
    protected function loopConfig($startTime): bool
    {
        if ($this->isDaemonized()) {
            return true;
        }

        return microtime(true) - $startTime < $this->timeout();
    }
}
