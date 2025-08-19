# Keboola Query Service API PHP Client

PHP client for Keboola Query Service API.

## Installation

```shell
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

// Health check
$health = $client->healthCheck();
```

## Configuration Options

The client constructor accepts the following configuration options:

- `url` (required): Query Service API URL (e.g., `https://query.keboola.com`)
- `token` (required): Storage API token
- `storageApiUrl` (optional): Storage API URL (auto-derived from Query Service URL if not provided)
- `backoffMaxTries` (optional): Number of retry attempts for failed requests (default: 3)
- `userAgent` (optional): Additional user agent string to append
- `handler` (optional): Custom Guzzle handler stack
- `logger` (optional): PSR-3 logger instance

## API Methods

- `submitQueryJob(string $branchId, string $workspaceId, array $requestBody): array`
- `getJobStatus(string $queryJobId): array`
- `getJobResults(string $queryJobId, string $statementId): array`
- `cancelJob(string $queryJobId, array $requestBody = []): array`
- `healthCheck(): array`
- `getStorageApiClient(): StorageApiClient`

## Development

### Requirements

- PHP 7.4+
- ext-json
- Composer

### Running Tests

Run tests:
```shell
composer run tests
```

### Code Quality

Run code style check:
```shell
composer run phpcs
```

Fix code style issues:
```shell
composer run phpcbf
```

Run static analysis:
```shell
composer run phpstan
```

Run all CI checks. Check [Github Workflows](./.github/workflows) for more details
```shell
composer run ci
```
