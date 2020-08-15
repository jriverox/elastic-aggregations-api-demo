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
      const numPartitions = distinctCount > pageSize ? Math.ceil(distinctCount / pageSize) : distinctCount;
      // const numPartitions = 3;
      const allPagesResult = [];
      for (let partition = 0; partition < numPartitions; partition++) {
        console.log('page:', partition);
        const response = await repository.getPagedBuckets(null, pageSize, partition, numPartitions);
        // console.log(response);
        const simplifiedArray = this.transform(response);
        // console.log(simplifiedArray);
        allPagesResult.push(...simplifiedArray);
      }
      // _.orderBy(allPagesResult, 'periodos.total', 'desc');
      // allPagesResult.sort((a, b) => b - a);
      res.json(allPagesResult);
    } catch (error) {
      res.status(500);
      next(error);
    }
  }

  transform(originalResponse) {
    console.log('begin transform');
    const buckets = originalResponse.aggregations.albums.buckets;
    console.log(JSON.stringify(buckets.length));
    const result = buckets.map((b) => {
      return this.transformItem(b);
    });
    console.log('end transform', result);
    return result;
  }

  transformItem(item) {
    const album = item.top.hits.hits[0]._source.album;
    // console.log(JSON.stringify(item));
    return {
      id: item.key,
      titulo: album.titulo,
      upc: album.upc,
      artista: album.artista ? album.artista.nombres : '',
      periodos: item.periodo.buckets.map((p) => {
        return {
          periodo: p.key_as_string,
          count: p.doc_count,
          total: p.ingreso_afiliado_nested.sum_monto_afiliado.value,
        };
      }),
    };
  }
};
