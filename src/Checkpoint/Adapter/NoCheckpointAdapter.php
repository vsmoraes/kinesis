<?php

namespace Vsmoraes\Kinesis\Checkpoint\Adapter;

use Vsmoraes\Kinesis\Checkpoint\Checkpoint;

class NoCheckpointAdapter implements Checkpoint
{
    /**
     * {@inheritdoc}
     */
    public function shardIteratorParams(string $streamName, string $shardId): array
    {
        return [
            'StreamName' => $streamName,
            'ShardId' => $shardId,
            'ShardIteratorType' => 'TRIM_HORIZON',
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function checkpoint(string $streamName, string $sequenceNumber)
    {
        // Do not save checkpoint
    }
}
