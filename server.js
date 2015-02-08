var express = require('express');
var bodyParser = require('body-parser');
var Matcher = require('./matcher').Matcher;
var app = express();

var MAX_REQUEST_SIZE = '1kb';

app.use(bodyParser.json({limit: MAX_REQUEST_SIZE}));

var matcher = new Matcher();
app.post('/event-match', function(req, res) {
  if (!req.body.time) {
    return res.status(400).json({
      error: 'Missing required parameter `time`'
    });
  }
  if (!req.body.latitude) {
    return res.status(400).json({
      error: 'Missing required parameter `latitude`'
    });
  }
  if (!req.body.longitude) {
    return res.status(400).json({
      error: 'Missing required parameter `longitude`'
    });
  }

  matcher.match(
    req.body.time,
    req.body.longitude,
    req.body.latitude,
    req.body.data
  ).then(function(match) {
    res.json(match);
  }).catch(function(error) {
    switch (error) {
      case 'no_match':
        return res.status(404).json({
          error: 'No match'
        });
        break;
      case 'queue_full':
        return res.status(503).json({
          error: 'Match queue full'
        });
        break;
      default:
        return res.status(500).json({
          error: 'Internal server error'
        });
    }
  }).done();
});

var port = process.env.PORT || 8888;
app.listen(port);
console.log("Running on port", port);
