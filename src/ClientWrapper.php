<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

class ClientWrapper extends Client
{
    /**
     * @inheritdoc
     */
    public function __construct(
        array $config,
        private readonly string $workspaceId,
        private readonly string $branchId,
    ) {
        parent::__construct($config);
    }

    /**
     * Submit a new query job
     *
     * @param array{statements: string[], transactional?: bool} $requestBody
     * @return array<string, mixed>
     */
    public function submitQueryJobWrapper(array $requestBody): array
    {
        return $this->submitQueryJob($this->branchId, $this->workspaceId, $requestBody);
    }

    /**
     * Execute a workspace query and wait for results
     *
     * @param array{statements: string[], transactional?: bool} $requestBody
     * @return array{
     *     queryJobId: string,
     *     status: string,
     *     statements: array<array<string, mixed>>,
     *     results: array{
     *          "columns": array<array{
     *              "name": string,
     *              "type": "text",
     *          }>,
     *          "data": array<array<int, string>>,
     *          "status": string,
     *          "rowsAffected": int
     *     }[],
     * }
     */
    public function executeWorkspaceQueryWrapper(
        array $requestBody,
        int $maxWaitSeconds = parent::DEFAULT_MAX_WAIT_SECONDS,
    ): array {
        return $this->executeWorkspaceQuery($this->branchId, $this->workspaceId, $requestBody, $maxWaitSeconds);
    }
}
