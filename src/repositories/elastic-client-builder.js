require('dotenv').config();
const AWS = require('aws-sdk');
// const _ = require('lodash');
const { Client } = require('elasticsearch');

const createClient = (host, region = 'us-east-1', requestTimeout = 30000, maxRetries = 3) => {
  // if (_.isEmpty(host)) throw new Error('Ivalid Host')

  return new Client({
    host: host,
    connectionClass: require('http-aws-es'),
    awsConfig: new AWS.Config({ region }),
    maxRetries: maxRetries,
    requestTimeout: requestTimeout,
  });
};

const createClientFromDefaultEnvVariables = () => {
  return createClient(
    process.env.ELASTIC_HOST,
    process.env.ELASTIC_REGION,
    process.env.ELASTIC_REQUEST_TIMEOUT,
    process.env.ELASTIC_MAX_RETRIES,
  );
};

module.exports = {
  createClient,
  createClientFromDefaultEnvVariables,
};
