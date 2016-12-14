# Kinesis Manager

A simple way to fetch data from Kinesis streams

[![Build Status](https://api.travis-ci.org/vsmoraes/kinesis.svg)](https://travis-ci.org/vsmoraes/kinesis) [![Latest Stable Version](https://poser.pugx.org/vsmoraes/kinesis/v/stable)](https://packagist.org/packages/vsmoraes/kinesis) [![Total Downloads](https://poser.pugx.org/vsmoraes/kinesis/downloads)](https://packagist.org/packages/vsmoraes/kinesis) [![Latest Unstable Version](https://poser.pugx.org/vsmoraes/kinesis/v/unstable)](https://packagist.org/packages/vsmoraes/kinesis) [![License](https://poser.pugx.org/vsmoraes/kinesis/license)](https://packagist.org/packages/vsmoraes/kinesis)

## Instalation
Add:
```
"vsmoraes/kinesis": "dev-master"
```
To your `composer.json`

or Run:
```
composer require vsmoraes/kinesis dev-master
```

## Example of usage
```php
<?php
include __DIR__ . '/vendor/autoload.php';

use Aws\Kinesis\KinesisClient;
use Vsmoraes\Kinesis\Checkpoint\Adapter\ResourceTagAdapter;
use Vsmoraes\Kinesis\Manager;

$kinesis = new KinesisClient([
    'region' => 'us-east-1',
    'version' => 'latest',
    'profile' => 'default'
]);
$checkpoint = new ResourceTagAdapter($kinesis);
$manager = new Manager($kinesis, $checkpoint);

$records = [];
foreach ($manager->records('stream-name-here') as $record) {
    $records[] = $record;
}

print_r($records);
```
