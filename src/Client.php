<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use JsonException;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Throwable;

class Client
{
    private const DEFAULT_USER_AGENT = 'Keboola Query API PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 3;
    private const GUZZLE_CONNECT_TIMEOUT_SECONDS = 10;
    private const GUZZLE_TIMEOUT_SECONDS = 120;

    private string $apiUrl;
    private string $tokenString;
    private int $backoffMaxTries;
    private string $userAgent;
    private GuzzleClient $client;
    private LoggerInterface $logger;
    private StorageApiClient $storageApiClient;

    /**
     * @param array{
     *     url: string,
     *     token: string,
     *     storageApiUrl?: string,
     *     backoffMaxTries?: int,
     *     userAgent?: string,
     *     handler?: HandlerStack,
     *     logger?: LoggerInterface,
     * } $config
     */
    public function __construct(array $config)
    {
        if (empty($config['url'])) {
            throw new InvalidArgumentException('url must be set');
        }
        if (empty($config['token'])) {
            throw new InvalidArgumentException('token must be set');
        }

        $this->apiUrl = rtrim($config['url'], '/');
        $this->tokenString = $config['token'];
        $this->backoffMaxTries = $config['backoffMaxTries'] ?? self::DEFAULT_BACKOFF_RETRIES;
        $this->userAgent = self::DEFAULT_USER_AGENT;
        $this->logger = $config['logger'] ?? new NullLogger();

        if (isset($config['userAgent'])) {
            $this->userAgent .= ' ' . $config['userAgent'];
        }

        $this->initStorageApiClient($config);

        // Allow override of Query API URL via environment variable for functional testing
        // This is specifically for overriding in functional tests when needed
        if (!empty($_ENV['TESTS_QUERY_API_URL_OVERRIDE'])) {
            $this->apiUrl = rtrim($_ENV['TESTS_QUERY_API_URL_OVERRIDE'], '/');
        }

        $this->initClient($config);
    }

    /**
     * @param array{handler?: HandlerStack} $config
     */
    private function initClient(array $config): void
    {
        $handlerStack = $config['handler'] ?? HandlerStack::create();
        $handlerStack->push(Middleware::retry($this->createRetryDecider(), $this->createRetryDelay()));

        $this->client = new GuzzleClient([
            'base_uri' => $this->apiUrl,
            'handler' => $handlerStack,
            'connect_timeout' => self::GUZZLE_CONNECT_TIMEOUT_SECONDS,
            'timeout' => self::GUZZLE_TIMEOUT_SECONDS,
        ]);
    }

    private function createRetryDecider(): callable
    {
        return function (
            int $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            ?Throwable $exception = null,
        ): bool {
            if ($retries >= $this->backoffMaxTries) {
                return false;
            }

            if ($exception instanceof ConnectException) {
                return true;
            }

            if ($response && $response->getStatusCode() >= 500) {
                return true;
            }

            return false;
        };
    }

    private function createRetryDelay(): callable
    {
        return function (int $numberOfRetries): int {
            return 1000 * (2 ** $numberOfRetries);
        };
    }

    /**
     * @param array{storageApiUrl?: string, logger?: LoggerInterface} $config
     */
    private function initStorageApiClient(array $config): void
    {
        $storageApiUrl = $config['storageApiUrl'] ?? $this->deriveStorageApiUrl();

        $this->storageApiClient = new StorageApiClient([
            'url' => $storageApiUrl,
            'token' => $this->tokenString,
            'logger' => $this->logger,
        ]);
    }

    private function deriveStorageApiUrl(): string
    {
        // Convert Query Service URL to Storage API URL
        // e.g., https://query.keboola.com -> https://connection.keboola.com
        // e.g., https://query.eu-central-1.keboola.com -> https://connection.eu-central-1.keboola.com
        $parsedUrl = parse_url($this->apiUrl);
        if ($parsedUrl === false) {
            throw new InvalidArgumentException('Invalid Query Service URL');
        }

        $scheme = $parsedUrl['scheme'] ?? 'https';
        $host = $parsedUrl['host'] ?? '';
        $port = isset($parsedUrl['port']) ? ':' . $parsedUrl['port'] : '';

        // Convert query.* subdomain to connection.* for Storage API
        if (strpos($host, 'query.') === 0) {
            $host = str_replace('query.', 'connection.', $host);
        }

        return sprintf('%s://%s%s', $scheme, $host, $port);
    }

    /**
     * Verify Storage API token before making Query Service requests
     */
    private function verifyStorageApiToken(): void
    {
        try {
            $this->storageApiClient->verifyToken();
        } catch (StorageApiClientException $e) {
            throw new ClientException(
                'Storage API token verification failed: ' . $e->getMessage(),
                $e->getCode(),
                $e,
            );
        }
    }

    /**
     * Submit a new query job
     *
     * @param array{statements: string[], transactional?: bool} $requestBody
     * @return array<string, mixed>
     */
    public function submitQueryJob(string $branchId, string $workspaceId, array $requestBody): array
    {
        $this->verifyStorageApiToken();
        $url = sprintf('/api/v1/branches/%s/workspaces/%s/queries', $branchId, $workspaceId);
        return $this->sendRequest('POST', $url, $requestBody);
    }

    /**
     * Get job status
     *
     * @return array<string, mixed>
     */
    public function getJobStatus(string $queryJobId): array
    {
        $this->verifyStorageApiToken();
        $url = sprintf('/api/v1/queries/%s', $queryJobId);
        return $this->sendRequest('GET', $url);
    }

    /**
     * Cancel a job
     *
     * @param array{reason?: string} $requestBody
     * @return array<string, mixed>
     */
    public function cancelJob(string $queryJobId, array $requestBody = []): array
    {
        $this->verifyStorageApiToken();
        $url = sprintf('/api/v1/queries/%s/cancel', $queryJobId);
        return $this->sendRequest('POST', $url, $requestBody);
    }

    /**
     * Get job results
     *
     * @return array<string, mixed>
     */
    public function getJobResults(string $queryJobId, string $statementId): array
    {
        $this->verifyStorageApiToken();
        $url = sprintf('/api/v1/queries/%s/%s/results', $queryJobId, $statementId);
        return $this->sendRequest('GET', $url);
    }

    /**
     * Health check
     *
     * @return array<string, mixed>
     */
    public function healthCheck(): array
    {
        return $this->sendRequest('GET', '/health-check');
    }

    /**
     * Get the Storage API client instance
     */
    public function getStorageApiClient(): StorageApiClient
    {
        return $this->storageApiClient;
    }

    /**
     * @param array<string, mixed>|null $requestBody
     * @return array<string, mixed>
     */
    private function sendRequest(string $method, string $url, ?array $requestBody = null): array
    {
        $headers = [
            'Content-Type' => 'application/json',
            'X-StorageAPI-Token' => $this->tokenString,
            'User-Agent' => $this->userAgent,
        ];

        $options = [
            'headers' => $headers,
        ];

        if ($requestBody !== null) {
            try {
                $options['body'] = json_encode($requestBody, JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                throw new ClientException('Failed to encode request body as JSON: ' . $e->getMessage(), 0, $e);
            }
        }

        try {
            $response = $this->client->request($method, $url, $options);
        } catch (GuzzleException $e) {
            $this->handleGuzzleException($e);
            throw new ClientException('Request failed after exception handling');
        }

        return $this->parseResponse($response);
    }

    /**
     * @return array<string, mixed>
     */
    private function parseResponse(ResponseInterface $response): array
    {
        $body = (string) $response->getBody();

        if (empty($body)) {
            return [];
        }

        try {
            $data = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new ClientException('Response is not valid JSON: ' . $e->getMessage(), 0, $e);
        }

        if (!is_array($data)) {
            throw new ClientException('Response is not a JSON object');
        }

        return $data;
    }

    /**
     * @throws ClientException
     */
    private function handleGuzzleException(GuzzleException $e): void
    {
        if ($e instanceof GuzzleClientException && $e->hasResponse()) {
            $response = $e->getResponse();
            $statusCode = $response->getStatusCode();
            $body = (string) $response->getBody();

            try {
                $errorData = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
            } catch (JsonException) {
                $errorData = null;
            }

            $message = is_array($errorData) && isset($errorData['exception']) && is_string($errorData['exception'])
                ? $errorData['exception']
                : $e->getMessage();
            $contextData = is_array($errorData) ? $errorData : null;
            throw new ClientException($message, $statusCode, $e, $contextData);
        }

        if ($e instanceof ConnectException) {
            throw new ClientException('Unable to connect to Query Service API: ' . $e->getMessage(), 0, $e);
        }

        throw new ClientException('Query Service API request failed: ' . $e->getMessage(), 0, $e);
    }
}
