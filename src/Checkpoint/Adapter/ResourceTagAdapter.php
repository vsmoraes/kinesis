<?php

namespace Vsmoraes\Kinesis\Checkpoint\Adapter;

use Aws\Kinesis\KinesisClient;
use Vsmoraes\Kinesis\Checkpoint\Checkpoint;

class ResourceTagAdapter implements Checkpoint
{
    const TAG_NAME = 'LastSequenceNumber';
    const DEFAULT_ITERATOR_TYPE = 'AFTER_SEQUENCE_NUMBER';

    /**
     * @var KinesisClient
     */
    private $kinesisClient;

    public function __construct(KinesisClient $kinesisClient)
    {
        $this->kinesisClient = $kinesisClient;
    }

    /**
     * {@inheritdoc}
     */
    public function shardIteratorParams(string $streamName, string $shardId): array
    {
        return [
            'StreamName' => $streamName,
            'ShardId' => $shardId,
            'ShardIteratorType' => self::DEFAULT_ITERATOR_TYPE,
            'StartingSequenceNumber' => $this->getSequenceNumberFromTags($streamName),
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function checkpoint(string $streamName, string $sequenceNumber)
    {
        $this->kinesisClient->addTagsToStream([
            'StreamName' => $streamName,
            'Tags' => [self::TAG_NAME => $sequenceNumber],
        ]);
    }

    /**
     * @param string $streamName
     *
     * @return string
     */
    protected function getSequenceNumberFromTags(string $streamName): string
    {
        $checkpoint = '';

        $tags = $this->kinesisClient->listTagsForStream([
            'StreamName' => $streamName,
        ])->get('Tags');

        foreach ($tags as $tag) {
            if ($tag['Key'] == self::TAG_NAME) {
                $checkpoint = $tag['Value'];

                break;
            }
        }

        return $checkpoint;
    }
}
