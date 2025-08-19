<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\QueryApi\Client;
use Keboola\QueryApi\ClientException;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

class ClientTest extends TestCase
{
    public function testConstructorRequiresUrl(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('url must be set');

        new Client([
            'token' => 'test-token',
        ]);
    }

    public function testConstructorRequiresToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('token must be set');

        new Client([
            'url' => 'https://test.keboola.com',
        ]);
    }

    public function testSubmitQueryJob(): void
    {
        $mockHandler = new MockHandler([
            new Response(201, [], json_encode(['queryJobId' => 'job-12345']) ?: ''),
        ]);

        $storageApiClient = $this->createMock(StorageApiClient::class);
        $storageApiClient->expects($this->once())->method('verifyToken');

        $client = $this->createClientWithMockHandler($mockHandler, $storageApiClient);

        $result = $client->submitQueryJob('main', 'workspace-123', [
            'statements' => ['SELECT * FROM table1'],
            'transactional' => true,
        ]);

        $this->assertEquals(['queryJobId' => 'job-12345'], $result);
    }

    public function testGetJobStatus(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'queryJobId' => 'job-12345',
                'status' => 'running',
                'statements' => [],
            ]) ?: ''),
        ]);

        $storageApiClient = $this->createMock(StorageApiClient::class);
        $storageApiClient->expects($this->once())->method('verifyToken');

        $client = $this->createClientWithMockHandler($mockHandler, $storageApiClient);

        $result = $client->getJobStatus('job-12345');

        $this->assertEquals('job-12345', $result['queryJobId']);
        $this->assertEquals('running', $result['status']);
    }

    public function testCancelJob(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode(['queryJobId' => 'job-12345']) ?: ''),
        ]);

        $storageApiClient = $this->createMock(StorageApiClient::class);
        $storageApiClient->expects($this->once())->method('verifyToken');

        $client = $this->createClientWithMockHandler($mockHandler, $storageApiClient);

        $result = $client->cancelJob('job-12345', ['reason' => 'User requested']);

        $this->assertEquals(['queryJobId' => 'job-12345'], $result);
    }

    public function testGetJobResults(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'data' => [['id' => 1, 'name' => 'test']],
                'status' => 'completed',
                'rowsAffected' => 1,
            ]) ?: ''),
        ]);

        $storageApiClient = $this->createMock(StorageApiClient::class);
        $storageApiClient->expects($this->once())->method('verifyToken');

        $client = $this->createClientWithMockHandler($mockHandler, $storageApiClient);

        $result = $client->getJobResults('job-12345', 'stmt-67890');

        $this->assertEquals('completed', $result['status']);
        $this->assertEquals(1, $result['rowsAffected']);
        assert(is_array($result['data']));
        $this->assertCount(1, $result['data']);
    }

    public function testHealthCheck(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'service' => 'query',
                'status' => 'ok',
                'timestamp' => '2024-01-01T00:00:00Z',
                'version' => '1.0.0',
            ]) ?: ''),
        ]);

        $storageApiClient = $this->createMock(StorageApiClient::class);
        // Health check should NOT verify token
        $storageApiClient->expects($this->never())->method('verifyToken');

        $client = $this->createClientWithMockHandler($mockHandler, $storageApiClient);

        $result = $client->healthCheck();

        $this->assertEquals('query', $result['service']);
        $this->assertEquals('ok', $result['status']);
    }

    public function testStorageApiTokenVerificationFailure(): void
    {
        $mockHandler = new MockHandler([]);

        $storageApiClient = $this->createMock(StorageApiClient::class);
        $storageApiClient->expects($this->once())
            ->method('verifyToken')
            ->willThrowException(new StorageApiClientException('Invalid token'));

        $client = $this->createClientWithMockHandler($mockHandler, $storageApiClient);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Storage API token verification failed: Invalid token');

        $client->submitQueryJob('main', 'workspace-123', [
            'statements' => ['SELECT * FROM table1'],
        ]);
    }

    public function testStorageApiUrlDerivation(): void
    {
        // Test with Query Service URL to ensure proper Storage API URL derivation
        $client = new Client([
            'url' => 'https://query.keboola.com',
            'token' => 'test-token',
        ]);

        // Since we can't easily mock the StorageApiClient constructor,
        // we'll just verify the client can be created without errors
        $this->assertInstanceOf(Client::class, $client);
    }

    private function createClientWithMockHandler(
        MockHandler $mockHandler,
        ?StorageApiClient $storageApiClient = null,
    ): Client {
        $handlerStack = HandlerStack::create($mockHandler);

        if ($storageApiClient) {
            // Use reflection to inject the mocked Storage API client
            $client = new Client([
                'url' => 'https://query.test.keboola.com',
                'token' => 'test-token',
                'handler' => $handlerStack,
            ]);

            $reflection = new ReflectionClass($client);
            $property = $reflection->getProperty('storageApiClient');
            $property->setAccessible(true);
            $property->setValue($client, $storageApiClient);

            return $client;
        }

        return new Client([
            'url' => 'https://query.test.keboola.com',
            'token' => 'test-token',
            'handler' => $handlerStack,
        ]);
    }
}
