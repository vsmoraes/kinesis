<?php

namespace Vsmoraes\Tests\Kinesis\Checkpoint\Adapter;

use Aws\Kinesis\KinesisClient;
use Aws\Result;
use Vsmoraes\Kinesis\Checkpoint\Adapter\ResourceTagAdapter;

class ResourceTagAdapterTest extends \PHPUnit_Framework_TestCase
{
    public function testShouldReturnShardIteratorParameters()
    {
        $streamName = 'foo';
        $shardId = 'bar';
        $sequenceNumber = 123456;
        $tags = [
            [
                'Key' => 'tag1',
                'Value' => 'value1',
             ],
            [
                'Key' => ResourceTagAdapter::TAG_NAME,
                'Value' => $sequenceNumber,
            ]
        ];

        $tagsResult = new Result([
            'Tags' => $tags,
        ]);

        $kinesisMock = $this->getMockBuilder(KinesisClient::class)
            ->setMethods(['listTagsForStream'])
            ->disableOriginalConstructor()
            ->getMockForAbstractClass();

        $kinesisMock->expects($this->once())
            ->method('listTagsForStream')
            ->willReturn($tagsResult);

        $adapter = new ResourceTagAdapter($kinesisMock);

        $result = $adapter->shardIteratorParams($streamName, $shardId);
        $expected = [
            'StreamName' => $streamName,
            'ShardId' => $shardId,
            'ShardIteratorType' => ResourceTagAdapter::DEFAULT_ITERATOR_TYPE,
            'StartingSequenceNumber' => $sequenceNumber,
        ];

        $this->assertEquals($expected, $result);
    }

    public function testShouldAddCheckpoint()
    {
        $streamName = 'foo';
        $sequenceNumber = 123456;

        $expectedParams = [
            'StreamName' => $streamName,
            'Tags' => [ResourceTagAdapter::TAG_NAME => $sequenceNumber],
        ];

        $kinesisMock = $this->getMockBuilder(KinesisClient::class)
            ->setMethods(['addTagsToStream'])
            ->disableOriginalConstructor()
            ->getMockForAbstractClass();

        $kinesisMock->expects($this->once())
            ->method('addTagsToStream')
            ->with($expectedParams);

        $adapter = new ResourceTagAdapter($kinesisMock);

        $adapter->checkpoint($streamName, $sequenceNumber);
    }
}
