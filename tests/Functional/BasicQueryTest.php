<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Functional;

class BasicQueryTest extends BaseFunctionalTestCase
{
    public function testSubmitSimpleSelectQuery(): void
    {
        // Test a simple SELECT query that doesn't require any tables
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT CURRENT_TIMESTAMP() AS "current_time"'],
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

        $this->assertArrayHasKey('status', $results);
        $this->assertEquals('completed', $results['status']);

        // Verify we got a timestamp result
        $this->assertArrayHasKey('data', $results);
        $data = $results['data'];
        assert(is_array($data));
        $this->assertCount(1, $data);
        $row = $data[0];
        assert(is_array($row));
        $this->assertCount(1, $row);
        // Query API returns indexed arrays, not associative arrays with column names
        $this->assertArrayHasKey(0, $row);
        $this->assertIsString($row[0]);
        $this->assertNotEmpty($row[0]);
        // Verify it's a valid timestamp (numeric string)
        $this->assertMatchesRegularExpression('/^\d+\.\d+$/', $row[0]);
    }

    public function testSubmitInformationSchemaQuery(): void
    {
        // Test a query against information_schema to verify database connectivity
        $response = $this->queryClient->submitQueryJob(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => [
                    'SELECT COUNT(*) AS "table_count" FROM information_schema.tables ' .
                    'WHERE table_schema = CURRENT_SCHEMA()',
                ],
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

        $this->assertArrayHasKey('status', $results);
        $this->assertEquals('completed', $results['status']);

        // Verify we got a count result
        $this->assertArrayHasKey('data', $results);
        $data = $results['data'];
        assert(is_array($data));
        $this->assertCount(1, $data);
        $row = $data[0];
        assert(is_array($row));
        $this->assertCount(1, $row);
        // Query API returns indexed arrays, not associative arrays with column names
        assert(isset($row[0]));
        $this->assertIsNumeric($row[0]);
        $this->assertGreaterThanOrEqual(0, (int) $row[0]);
    }

    public function testExecuteWorkspaceQuery(): void
    {
        // Test the new executeWorkspaceQuery method with a simple query
        $response = $this->queryClient->executeWorkspaceQuery(
            $this->getTestBranchId(),
            $this->getTestWorkspaceId(),
            [
                'statements' => ['SELECT CURRENT_TIMESTAMP() AS "current_time"'],
                'transactional' => false,
            ],
        );

        // Verify the response structure
        $this->assertArrayHasKey('queryJobId', $response);
        $this->assertArrayHasKey('status', $response);
        $this->assertArrayHasKey('statements', $response);
        $this->assertArrayHasKey('results', $response);

        // Verify job completed successfully
        $this->assertEquals('completed', $response['status']);
        $this->assertNotEmpty($response['queryJobId']);

        // Verify statements
        $statements = $response['statements'];
        assert(is_array($statements));
        $this->assertCount(1, $statements);

        $statement = $statements[0];
        assert(is_array($statement));
        $this->assertEquals('completed', $statement['status']);

        // Verify results
        $results = $response['results'];
        assert(is_array($results));
        $this->assertCount(1, $results);

        $result = $results[0];
        assert(is_array($result));
        $this->assertEquals('completed', $result['status']);

        // Verify we got timestamp data
        $this->assertArrayHasKey('data', $result);
        $data = $result['data'];
        assert(is_array($data));
        $this->assertCount(1, $data);
        $row = $data[0];
        assert(is_array($row));
        $this->assertCount(1, $row);
        // Query API returns indexed arrays, not associative arrays with column names
        $this->assertArrayHasKey(0, $row);
        $this->assertIsString($row[0]);
        $this->assertNotEmpty($row[0]);
        // Verify it's a valid timestamp (numeric string)
        $this->assertMatchesRegularExpression('/^\d+\.\d+$/', $row[0]);
    }
}
