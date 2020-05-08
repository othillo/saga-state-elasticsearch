<?php

/*
 * This file is part of the broadway/saga-state-elasticsearch package.
 *
 * (c) 2020 Broadway project
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Broadway\Saga\State\Elasticsearch;

use Broadway\Saga\State;
use Broadway\Saga\State\Criteria;
use Broadway\Saga\State\RepositoryException;
use Broadway\Saga\State\RepositoryInterface;
use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;

class ElasticsearchRepository implements RepositoryInterface
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $index;

    public function __construct(Client $client, string $index)
    {
        $this->client = $client;
        $this->index = $index;
    }

    public function findOneBy(Criteria $criteria, string $sagaId): ?State
    {
        $query = [
            'index' => $this->index,
            'type' => $sagaId,
            'body' => [
                'query' => [
                    'bool' => [
                        'must' => $this->buildFilter($criteria->getComparisons()),
                    ],
                ],
            ],
            'size' => 500,
        ];

        $results = $this->searchAndDeserializeHits($query);
        $count = count($results);

        if (1 === $count) {
            return current($results);
        }

        if ($count > 1) {
            throw new RepositoryException('Multiple saga state instances found.');
        }

        return null;
    }

    public function save(State $state, string $sagaId): void
    {
        $params = [
            'index' => $this->index,
            'type' => $sagaId,
            'id' => $state->getId(),
            'body' => $state->serialize(),
            'refresh' => true,
        ];

        $this->client->index($params);
    }

    private function buildFilter(array $filter): array
    {
        $retval = [];

        foreach ($filter as $field => $value) {
            $retval[] = ['match' => ['values.'.$field => $value]];
        }

        $retval[] = ['match' => ['done' => false]];

        return $retval;
    }

    private function searchAndDeserializeHits(array $query): array
    {
        try {
            $result = $this->client->search($query);
        } catch (Missing404Exception $e) {
            return [];
        }

        if (!array_key_exists('hits', $result)) {
            return [];
        }

        return array_map(function ($serializedSagaState) {
            return State::deserialize($serializedSagaState['_source']);
        }, $result['hits']['hits']);
    }
}
