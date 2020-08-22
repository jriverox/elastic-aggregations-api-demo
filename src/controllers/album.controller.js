/* eslint-disable require-jsdoc */
const _ = require('lodash');
const AlbumRepository = require('../repositories/album.repository');
const elatiscClientBuilder = require('../repositories/elastic-client-builder');
const elasticClient = elatiscClientBuilder.createClientFromDefaultEnvVariables();

const repository = new AlbumRepository(elasticClient);

const pageSize = 150;

module.exports = class AlbumController {
  async getAllBuckets(req, res, next) {
    try {
      console.log('entro');
      const distinctCount = await repository.getDistinctCount();
      console.log(distinctCount);
      //const numPartitions = distinctCount > pageSize ? Math.ceil(distinctCount / pageSize) : distinctCount;
      const numPartitions = 4;
      let allPagesResult = [];
      for (let partition = 0; partition < numPartitions; partition++) {
        console.log('page:', partition);
        const response = await repository.getPagedBuckets(null, pageSize, partition, numPartitions);
        const simplifiedArray = this.transform(response);
        allPagesResult.push(...simplifiedArray);
      }
      allPagesResult = _.orderBy(allPagesResult, ['total', 'periodo', 'titulo'], ['desc', 'desc', 'asc']);
      res.json(allPagesResult);
    } catch (error) {
      res.status(500);
      next(error);
    }
  }

  transform(originalResponse) {
    const buckets = originalResponse.aggregations.albums.buckets;
    const items = [];
    buckets.forEach((bucket) => {
      items.push(...this.transformItem(bucket));
    });
    return items;
  }

  transformItem(item) {
    const album = item.top.hits.hits[0]._source.album;
    const artista = album.artista ? album.artista.nombres : '';

    return item.periodo.buckets.map((periodo) => {
      return {
        id: item.key,
        titulo: album.titulo || '',
        upc: album.upc || '',
        artista: artista,
        periodo: periodo.key_as_string,
        total: periodo.ingreso_afiliado_nested.sum_monto_afiliado.value,
      };
    });
  }
};
