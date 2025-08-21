<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

use Keboola\QueryApi\Client;
use Keboola\QueryApi\ClientException;

class QueryServiceFunctionalTest extends BaseFunctionalTestCase
{
    public function testHealthCheck(): void
    {
        $result = $this->queryClient->healthCheck();

        $this->assertArrayHasKey('service', $result);
        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('timestamp', $result);
        $this->assertArrayHasKey('version', $result);

        $this->assertEquals('query', $result['service']);
        $this->assertEquals('ok', $result['status']);
    }

    public function testSubmitAndGetSimpleQuery(): void
    {
        // Create test table with sample data
        $tableName = $this->createTestTable();

        // Submit a simple SELECT query
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [sprintf('SELECT COUNT(*) as row_count FROM %s', $tableName)],
                'transactional' => false,
            ],
        );

        $this->assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));
        $this->assertNotEmpty($queryJobId);

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        $this->assertEquals('completed', $finalStatus['status']);
        $this->assertEquals($queryJobId, $finalStatus['queryJobId']);
        $this->assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        $this->assertCount(1, $statements);

        $statement = $statements[0];
        assert(is_array($statement));
        $this->assertEquals('completed', $statement['status']);

        // Get job results
        $this->assertArrayHasKey('id', $statement);
        $results = $this->queryClient->getJobResults($queryJobId, $statement['id']);

        $this->assertArrayHasKey('data', $results);
        $this->assertArrayHasKey('status', $results);
        $this->assertEquals('completed', $results['status']);

        // Verify the result contains our count
        $this->assertArrayHasKey('data', $results);
        $data = $results['data'];
        assert(is_array($data));
        $this->assertCount(1, $data);
        $row = $data[0];
        assert(is_array($row));
        $this->assertEquals(3, $row[0]); // We inserted 3 rows
    }

    public function testSubmitTransactionalQuery(): void
    {
        // Create test table
        $tableName = $this->createTestTable();

        // Submit transactional queries (INSERT and SELECT)
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    sprintf('INSERT INTO %s (id, name, value) VALUES (4, \'test4\', 400)', $tableName),
                    sprintf('SELECT COUNT(*) as row_count FROM %s', $tableName),
                ],
                'transactional' => true,
            ],
        );

        $this->assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));

        // Wait for completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        $this->assertEquals('completed', $finalStatus['status']);
        $this->assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        $this->assertCount(2, $statements);

        // Check INSERT statement
        $insertStatement = $statements[0];
        assert(is_array($insertStatement));
        $this->assertEquals('completed', $insertStatement['status']);

        // Check SELECT statement and its results
        $selectStatement = $statements[1];
        assert(is_array($selectStatement));
        $this->assertEquals('completed', $selectStatement['status']);

        $this->assertArrayHasKey('id', $selectStatement);
        $results = $this->queryClient->getJobResults($queryJobId, $selectStatement['id']);
        $this->assertArrayHasKey('data', $results);
        $data = $results['data'];
        assert(is_array($data));
        $row = $data[0];
        assert(is_array($row));
        $this->assertEquals(4, $row[0]); // Should be 4 rows now
    }

    public function testCancelQueryJob(): void
    {
        // Create test table
        $tableName = $this->createTestTable();

        // Submit a cross join query that takes some time to process
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    sprintf('
                        SELECT a.id, b.id as id2, a.name, b.name as name2
                        FROM %s a
                        CROSS JOIN %s b
                        CROSS JOIN %s c
                        ORDER BY 1, 2
                    ', $tableName, $tableName, $tableName),
                ],
                'transactional' => false,
            ],
        );

        $this->assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));
        $this->assertNotEmpty($queryJobId);

        // Cancel the job
        $cancelResponse = $this->queryClient->cancelJob($queryJobId, [
            'reason' => 'Test cancellation',
        ]);

        $this->assertEquals($queryJobId, $cancelResponse['queryJobId']);

        // Wait for final status
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId, 15);

        // Job should be canceled
        $this->assertEquals('canceled', $finalStatus['status']);
        $this->assertArrayHasKey('cancellationReason', $finalStatus);
        $this->assertEquals('Test cancellation', $finalStatus['cancellationReason']);
        $this->assertArrayHasKey('canceledAt', $finalStatus);

        // Verify job has statements but don't assert on their status
        $this->assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        $this->assertCount(1, $statements);
    }

    public function testQueryJobWithInvalidSQL(): void
    {
        // Submit query with invalid SQL
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
            'statements' => ['SELECT * FROM non_existent_table_12345'],
            'transactional' => false,
            ],
        );

        $this->assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        // Job should fail due to invalid SQL
        $this->assertEquals('failed', $finalStatus['status']);
        $this->assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        $this->assertCount(1, $statements);

        // The statement remains in 'waiting' status because the job failed before execution
        $statement = $statements[0];
        assert(is_array($statement));
        $this->assertEquals('completed', $statement['status']);
        assert(is_string($statement['query']));
        $this->assertEquals('SELECT * FROM non_existent_table_12345', $statement['query']);
    }

    public function testQueryJobWithEmptyStatements(): void
    {
        $this->expectException(ClientException::class);

        $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
            'statements' => [],
            'transactional' => false,
            ],
        );
    }

    public function testQueryJobWithInvalidBranch(): void
    {
        // Submit job with an invalid branch ID
        $response = $this->queryClient->submitQueryJob(
            'non-existent-branch-12345',
            $this->getTestWorkspaceId(),
            [
            'statements' => ['SELECT 1'],
            'transactional' => false,
            ],
        );

        $this->assertArrayHasKey('queryJobId', $response);
        $queryJobId = $response['queryJobId'];
        assert(is_string($queryJobId));

        // Wait for job completion
        $finalStatus = $this->queryClient->waitForJobCompletion($queryJobId);

        // Query Service accepts invalid branch IDs and executes successfully
        $this->assertEquals('completed', $finalStatus['status']);
        $this->assertArrayHasKey('statements', $finalStatus);
        $statements = $finalStatus['statements'];
        assert(is_array($statements));
        $this->assertCount(1, $statements);

        $statement = $statements[0];
        assert(is_array($statement));
        $this->assertEquals('completed', $statement['status']);
        assert(is_string($statement['query']));
        $this->assertEquals('SELECT 1', $statement['query']);
        assert(is_int($statement['rowsAffected']));
        $this->assertEquals(0, $statement['rowsAffected']);
    }

    public function testQueryJobWithInvalidWorkspace(): void
    {
        $this->expectException(ClientException::class);

        $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            'non-existent-workspace-12345',
            [
            'statements' => ['SELECT 1'],
            'transactional' => false,
            ],
        );
    }

    public function testGetJobStatusForNonExistentJob(): void
    {
        $this->expectException(ClientException::class);

        $this->queryClient->getJobStatus('non-existent-job-12345');
    }

    public function testGetJobResultsForNonExistentJob(): void
    {
        $this->expectException(ClientException::class);

        $this->queryClient->getJobResults('non-existent-job-12345', 'non-existent-statement-12345');
    }

    public function testCancelNonExistentJob(): void
    {
        $this->expectException(ClientException::class);

        $this->queryClient->cancelJob('non-existent-job-12345', ['reason' => 'Test']);
    }

    public function testInvalidStorageToken(): void
    {
        // Create a client with an invalid storage token
        $invalidTokenClient = new Client([
            'url' => $_ENV['TESTS_QUERY_API_URL'],
            'token' => 'invalid-token-12345',
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Authentication failed');

        // Attempt to submit a query job with invalid token
        $invalidTokenClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT 1'],
            ],
        );
    }
}
