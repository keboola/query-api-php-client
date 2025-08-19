# Keboola Query Service API PHP Client

PHP client for Keboola Query Service API.

## Installation

```bash
composer require keboola/query-api-php-client
```

## Usage

```php
<?php

use Keboola\QueryApi\Client;

$client = new Client([
    'url' => 'https://query.keboola.com',
    'token' => 'your-storage-api-token'
]);

// Submit a query job
$response = $client->submitQueryJob('main', 'workspace-123', [
    'statements' => ['SELECT * FROM table1'],
    'transactional' => true
]);

$queryJobId = $response['queryJobId'];

// Get job status
$status = $client->getJobStatus($queryJobId);

// Get job results
$results = $client->getJobResults($queryJobId, $statementId);

// Cancel job
$client->cancelJob($queryJobId, ['reason' => 'User requested cancellation']);
```

## Development

### Running Tests

Run unit tests:
```bash
composer tests
```

Run functional tests (requires .env file with test configuration):
```bash
composer tests-functional
```

Run all tests:
```bash
composer tests-all
```

### Functional Tests Setup

For functional tests, copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Then edit `.env` with your test environment settings:

```env
TESTS_HOSTNAME_SUFFIX=.keboola.com
TESTS_STORAGE_API_TOKEN=your-storage-api-token
```

**Note**: Functional tests will create and delete temporary branches and workspaces in your Keboola project. Make sure to use a development/test project with appropriate permissions.

### Code Quality

Run code style check:
```bash
composer phpcs
```

Run static analysis:
```bash
composer phpstan
```

Run all CI checks:
```bash
composer ci
```