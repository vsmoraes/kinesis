<?php

namespace Vsmoraes\Tests\Kinesis;

use Aws\Kinesis\KinesisClient;
use Aws\Result;
use Vsmoraes\Kinesis\Checkpoint\Checkpoint;
use Vsmoraes\Kinesis\Manager;

class ManagerTest extends \PHPUnit_Framework_TestCase
{
    public function testShouldRetriveRecordsFromStream()
    {
        $streamName = 'stream-foo';

        $shardIterator = 'foo';
        $shardIteratorResult = new Result([
            'ShardIterator' => $shardIterator,
        ]);

        $records = [
            [
                'SequenceNumber' => 123,
                'Data' => json_encode(['foo' => 'bar'], true),
            ]
        ];
        $recordsResult = new Result([
            'Records' => $records,
            'NextShardIterator' => '2eiwojfoij209fjioefw',
        ]);

        $kinesisMock = $this->getMockBuilder(KinesisClient::class)
            ->setMethods(['getShardIterator', 'getRecords'])
            ->disableOriginalConstructor()
            ->getMockForAbstractClass();

        $kinesisMock->expects($this->once())
            ->method('getShardIterator')
            ->willReturn($shardIteratorResult);

        $kinesisMock->expects($this->once())
            ->method('getRecords')
            ->with(['ShardIterator' => $shardIterator, 'Limit' => Manager::DEFAULT_LIMIT])
            ->willReturn($recordsResult);

        $checkpointMock = $this->getMockBuilder(Checkpoint::class)
            ->setMethods(['shardIteratorParams'])
            ->getMockForAbstractClass();

        $checkpointMock->expects($this->once())
            ->method('shardIteratorParams')
            ->willReturn(['foo']);

        $manager = new Manager($kinesisMock, $checkpointMock, Manager::DEFAULT_LIMIT, 0.0001);

        $result = [];
        foreach ($manager->records($streamName) as $record) {
            $result[] = $record;
        }
    }
}
