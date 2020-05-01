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

use Broadway\Saga\State\RepositoryInterface;
use Broadway\Saga\State\Testing\AbstractRepositoryTest;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class ElasticsearchRepositoryTest extends AbstractRepositoryTest
{
    public const INDEX = 'saga_state';

    /**
     * @var Client
     */
    private $client;

    public function setUp(): void
    {
        $this->client = ClientBuilder::fromConfig(['hosts' => ['localhost:9200']]);
        $this->client->indices()->create(['index' => self::INDEX]);
        $this->client->cluster()->health(['index' => self::INDEX, 'wait_for_status' => 'yellow', 'timeout' => '10s']);

        parent::setUp();
    }

    protected function createRepository(): RepositoryInterface
    {
        return new ElasticsearchRepository($this->client, self::INDEX);
    }

    public function tearDown(): void
    {
        $this->client->indices()->delete(['index' => self::INDEX]);
    }
}
