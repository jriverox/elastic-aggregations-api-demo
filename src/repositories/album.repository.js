/* eslint-disable require-jsdoc */
const elasticsearch = require('elasticsearch');
const aggregationsPartitionsQuery = require('./elastic-queries/album-aggregations-partitions.query.json');

module.exports = class AlbumRepository {
  constructor(elasticClient) {
    if (!elasticClient || elasticClient instanceof elasticsearch.Client) {
      throw new Error('elasticClient debe ser un cliente valido de elastic.');
    }
    this.client = elasticClient;
    this.index = process.env.ELASTIC_INDEX_ALIAS;
    this.type = process.env.ELASTIC_INDEX_TYPE;
  }

  async getDistinctCount() {
    const aggs = {
      albums_count: {
        cardinality: {
          field: 'album.id',
        },
      },
    };

    const result = await this.client.search({
      index: this.index,
      type: this.type,
      size: 0,
      body: {
        aggs: aggs,
      },
    });

    return result.aggregations.albums_count.value;
  }

  prepareAggsPartitionsQuery(size = 0, partition = 0, numPartitions = 0) {
    const aggs = aggregationsPartitionsQuery;
    aggs.albums.terms.size = size;
    aggs.albums.terms.include.partition = partition;
    aggs.albums.terms.include.num_partitions = numPartitions;
    return aggs;
  }

  async getPagedBuckets(query, size = 0, partition = 0, numPartitions = 0) {
    const aggs = this.prepareAggsPartitionsQuery(size, partition, numPartitions);

    const result = await this.client.search({
      index: this.index,
      type: this.type,
      size: 0,
      body: {
        aggs: aggs,
      },
    });
    return result;
  }
};
