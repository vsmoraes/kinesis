<?php

namespace Vsmoraes\Kinesis;

use Aws\Kinesis\KinesisClient;
use Vsmoraes\Kinesis\Checkpoint\Checkpoint;

class Manager
{
    const DEFAULT_LIMIT = 1000;
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

    public function __construct(
        KinesisClient $kinesisClient,
        Checkpoint $checkpoint,
        int $limit = self::DEFAULT_LIMIT,
        float $timeout = self::TIMEOUT
    ) {
        $this->kinesisClient = $kinesisClient;
        $this->checkpoint = $checkpoint;
        $this->limit = $limit;
        $this->timeout = $timeout;
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

        while (microtime(true) - $startTime < $this->timeout()) {
            $recordResponse = $this->kinesisClient->getRecords([
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
     * @return float
     */
    public function timeout(): float
    {
        return $this->timeout;
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

            $result = $this->kinesisClient->getShardIterator($checkpoint);
        } catch (\Exception $exception) {
            $result = $this->kinesisClient->getShardIterator([
                'StreamName' => $streamName,
                'ShardId' => self::SHARD_ID,
                'ShardIteratorType' => 'TRIM_HORIZON',
            ]);
        }

        return $result->get('ShardIterator');
    }
}
