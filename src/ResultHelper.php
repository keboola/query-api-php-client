<?php

declare(strict_types=1);

namespace Keboola\QueryApi;

class ResultHelper
{
    /**
     * @param array{
     *      "columns": array<int, array{
     *          "name": string,
     *          "type": "text",
     *      }>,
     *      "data": array<array<int, string>>,
     * } $responseData
     * @return array{
     *       "columns": array<int, array{
     *           "name": string,
     *           "type": "text",
     *       }>,
     *       "data": array<array<string, string>>,
     *   }
     */
    public static function mapColumnNamesIntoData(array $responseData): array
    {
        $data = $responseData['data'];
        $columnNames = array_column($responseData['columns'], 'name');

        $transformedData = [];
        foreach ($data as $row) {
            assert(is_array($row));
            $transformedRow = [];
            foreach ($row as $index => $value) {
                if (isset($columnNames[$index])) {
                    $transformedRow[$columnNames[$index]] = $value;
                }
            }
            $transformedData[] = $transformedRow;
        }
        $responseData['data'] = $transformedData;
        return $responseData;
    }
}
