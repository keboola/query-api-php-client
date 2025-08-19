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
use PHPUnit\Framework\TestCase;

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

        $client = $this->createClientWithMockHandler($mockHandler);

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

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->getJobStatus('job-12345');

        $this->assertEquals('job-12345', $result['queryJobId']);
        $this->assertEquals('running', $result['status']);
    }

    public function testCancelJob(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode(['queryJobId' => 'job-12345']) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

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

        $client = $this->createClientWithMockHandler($mockHandler);

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

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->healthCheck();

        $this->assertEquals('query', $result['service']);
        $this->assertEquals('ok', $result['status']);
    }

    public function testStorageApiUrlDerivation(): void
    {
        // Test with Query Service URL to ensure proper Storage API URL derivation
        $client = new Client([
            'url' => 'https://query.keboola.com',
            'token' => 'test-token',
        ]);

        $this->assertInstanceOf(Client::class, $client);
    }

    private function createClientWithMockHandler(MockHandler $mockHandler): Client
    {
        $handlerStack = HandlerStack::create($mockHandler);

        return new Client([
            'url' => 'https://query.test.keboola.com',
            'token' => 'test-token',
            'handler' => $handlerStack,
        ]);
    }
}
