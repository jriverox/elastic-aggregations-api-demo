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

  async getBucketDistinctCount() {
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

  async getPagedBuckets(params, size = 0, partition = 0, numPartitions = 0) {
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

  async bulkDocuments(documents, indexName) {
    const body = this.getBulkBody(documents, indexName);
    return await this.client.bulk({ body });
  }

  getBulkBody(documents, indexName) {
    const body = [];
    for (const document of documents) {
      const item = this.getBulkItem(document.id, document, indexName);
      body.push(item.header);
      body.push(item.doc);
    }
    return body;
  }

  getBulkItem(id, doc, indexName) {
    const cloneDoc = Object.assign({}, doc);
    delete cloneDoc.id;

    return {
      header: {
        update: {
          _index: indexName,
          _type: '_doc',
          _id: id,
        },
      },
      doc: {
        doc: cloneDoc,
        doc_as_upsert: true,
      },
    };
  }

  async cacheHasData(partitionKey, indexName) {
    const exists = await this.client.indices.exists({ index: indexName });
    if (!exists) return false;

    const result = await this.client.count({
      index: indexName,
      body: {
        query: this.getQuery(partitionKey),
      },
    });

    return result.count > 0;
  }

  async getDocumentsFromCache(partitionKey, indexName, size = 10, from = 0) {
    const exists = await this.client.indices.exists({ index: indexName });
    if (!exists) return false;

    const result = await this.client.search({
      index: indexName,
      body: {
        size: size,
        from: from,
        query: this.getQuery(partitionKey),
        sort: [
          {
            total: {
              order: 'desc',
            },
          },
          {
            periodo: {
              order: 'desc',
            },
          },
        ],
      },
    });
    if (result.hits.hits.length === 0) return [];

    return result.hits.hits.map((item) => {
      return item._source;
    });
  }

  getQuery(partitionKey) {
    return {
      term: { 'partitionKey.keyword': partitionKey },
    };
  }
};
