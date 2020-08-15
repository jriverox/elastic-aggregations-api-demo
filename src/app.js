const express = require('express');
const app = express();
const AlbumController = require('./controllers/album.controller');
const albumController = new AlbumController();

app.get('/', albumController.getAllBuckets.bind(albumController));

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send(err.message);
  res.render('error', { error: err });
});

module.exports = app;
