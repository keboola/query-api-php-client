<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit;

use Keboola\QueryApi\ResultHelper;
use PHPUnit\Framework\TestCase;

class ResultHelperTest extends TestCase
{
    public function testMapColumnNamesIntoData(): void
    {
        $input = [
            'columns' => [
                ['name' => 'id', 'type' => 'text'],
                ['name' => 'name', 'type' => 'text'],
                ['name' => 'city', 'type' => 'text'],
            ],
            'data' => [
                ['1', 'Alice', 'Prague'],
                ['2', 'Bob', 'Liberec'],
                ['3', 'Charlie', 'Brno', 'EXTRA'],
            ],
        ];

        $expected = [
            'columns' => [
                ['name' => 'id', 'type' => 'text'],
                ['name' => 'name', 'type' => 'text'],
                ['name' => 'city', 'type' => 'text'],
            ],
            'data' => [
                ['id' => '1', 'name' => 'Alice', 'city' => 'Prague'],
                ['id' => '2', 'name' => 'Bob', 'city' => 'Liberec'],
                ['id' => '3', 'name' => 'Charlie', 'city' => 'Brno'],
            ],
        ];

        $actual = ResultHelper::mapColumnNamesIntoData($input);

        // Columns should be preserved unchanged
        self::assertSame($expected['columns'], $actual['columns']);
        // Data rows should be mapped by column names
        self::assertSame($expected['data'], $actual['data']);
    }
}
