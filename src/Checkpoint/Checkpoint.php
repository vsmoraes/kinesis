<?php

namespace Vsmoraes\Kinesis\Checkpoint;

interface Checkpoint
{
    /**
     * Will return all parameters necessary to retrieve the shard iterator
     *
     * @param string $streamName
     * @param string $shardId
     *
     * @return array
     */
    public function shardIteratorParams(string $streamName, string $shardId): array;

    /**
     * Save the sequence number as the checkpoint
     *
     * @param string $streamName
     * @param string $sequenceNumber
     *
     * @return void
     */
    public function checkpoint(string $streamName, string $sequenceNumber);
}
