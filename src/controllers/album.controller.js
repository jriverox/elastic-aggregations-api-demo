/* eslint-disable require-jsdoc */
const _ = require('lodash');
const AlbumRepository = require('../repositories/album.repository');
const elatiscClientBuilder = require('../repositories/elastic-client-builder');
const elasticClient = elatiscClientBuilder.createClientFromDefaultEnvVariables();

const repository = new AlbumRepository(elasticClient);

const aggregationPageSize = 150;
const cacheIndexName = 'album_cache';

module.exports = class AlbumController {
  async getSummary(req, res, next) {
    try {
      const params = this.getParams(req);
      const partitionKey = this.getPartitionKey(params);
      const cacheHasData = await repository.cacheHasData(partitionKey, cacheIndexName);
      if (!cacheHasData) {
        await this.buildCache(params, partitionKey);
      }
      const data = await repository.getDocumentsFromCache(partitionKey, cacheIndexName, params.size, params.from);
      res.json(data);
    } catch (error) {
      res.status(500);
      next(error);
    }
  }

  async buildCache(params, partitionKey) {
    const distinctCount = await repository.getBucketDistinctCount(params);
    console.log(distinctCount);
    const numPartitions = distinctCount > aggregationPageSize ? Math.ceil(distinctCount / aggregationPageSize) : distinctCount;
    // const numPartitions = 4;
    for (let partition = 0; partition < numPartitions; partition++) {
      const response = await repository.getPagedBuckets(params, aggregationPageSize, partition, numPartitions);
      const simplifiedArray = this.transform(response, partitionKey);
      await this.saveToCache(simplifiedArray);
    }
  }

  getParams(request) {
    const periodoDesde = request.periodoDesde || '*';
    const periodoHasta = request.periodoHasta || '*';
    const afiliadoId = request.afiliadoId || 0;
    const sello = request.sello || 0;
    const track = request.track || 0;
    const album = request.album || 0;
    const artista = request.artista || 0;
    const pais = request.pais || 0;
    const canal = request.canal || 0;
    const busqueda = request.busqueda || '*';
    const size = request.size || 10;
    const page = request.page || 1;

    return {
      periodoDesde,
      periodoHasta,
      afiliadoId,
      sello,
      track,
      album,
      artista,
      pais,
      canal,
      busqueda,
      size,
      page,
    };
  }

  getPartitionKey(params) {
    const periodoDesde = params.periodoDesde || '*';
    const periodoHasta = params.periodoHasta || '*';
    const afiliadoId = params.afiliadoId || 0;
    const sello = params.sello || 0;
    const track = params.track || 0;
    const album = params.album || 0;
    const artista = params.artista || 0;
    const pais = params.pais || 0;
    const canal = params.canal || 0;
    const busqueda = params.busqueda || '*';

    return `${periodoDesde}.${periodoHasta}.${afiliadoId}.${sello}.${track}.${album}.${artista}.${pais}.${canal}.${busqueda}`;
  }

  transform(originalResponse, partitionKey) {
    const currentDate = new Date();
    const buckets = originalResponse.aggregations.albums.buckets;
    const items = [];
    buckets.forEach((bucket) => {
      items.push(...this.transformItem(bucket, partitionKey, currentDate));
    });
    return items;
  }

  transformItem(item, partitionKey, date) {
    const album = item.top.hits.hits[0]._source.album;
    const artista = album.artista ? album.artista.nombres : '';

    return item.periodo.buckets.map((periodo) => {
      return {
        id: `${partitionKey}.${item.key}`,
        partitionKey: partitionKey,
        key: item.key,
        titulo: album.titulo || '',
        upc: album.upc || '',
        artista: artista,
        periodo: periodo.key_as_string,
        total: periodo.ingreso_afiliado_nested.sum_monto_afiliado.value,
        date: date,
      };
    });
  }

  async saveToCache(documents) {
    // evaluar resultado
    return await repository.bulkDocuments(documents, cacheIndexName);
  }
};
