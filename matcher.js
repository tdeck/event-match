var Q = require('q'),
    geolib = require('geolib'),
    uuid = require('node-uuid');

// All time values are in milliseconds
// TODO comment
module.exports.Matcher = Matcher;

function Matcher(opts) {
  opts = opts || {};
  // Only group requests received within this time window of each other.
  // Note that this means we need to keep requests in the queue for 
  // _MAX_REQUEST_SKEW milliseconds at a minimum in order to ensure we make
  // all valid matches.
  this._MAX_REQUEST_SKEW = opts.max_request_skew || 3000;
  // Only group events within this time range.
  this._MAX_CLOCK_SKEW = opts.max_clock_skew || 100;
  // Once the queue becomes full, we start dropping requests and returning 503.
  // NOTE: When setting this, note that V8 often does not deal well with 
  // processes using more than about 1GB of memory, in my experience.
  this._MAX_QUEUE_SIZE = opts.max_queue_size || 1000;
  // Only match events geographically this distance apart (in meters).
  this._MAX_DISTANCE = opts.max_distance || 100;

  // How often to process matches in the event queue.
  this._INTERVAL = opts.interval || 400;
  
  // This is a FIFO queue containing event objects
  // Each event object looks like this:
  // {
  //   request_time: // (ms) time request was enqueued
  //   event_time: // (ms) Event time from client
  //   location: {
  //     longitude:
  //     latitude:
  //   }
  //   data_item:
  //   promise:
  // }
  // Note that this is meant to be clear, not efficient.
  this._event_queue = [];

  this._interval_id = null;
};

// Start running the matcher at intervals (idempotent)
Matcher.prototype.start = function() {
  if (this._interval_id !== null) return;

  this._interval_id = setInterval(this._process_batch.bind(this), this._INTERVAL);
};

// Stop running the matcher at intervals (idempotent)
Matcher.prototype.stop = function() {
  if (this._interval_id === null) return;

  clearInterval(this._interval_id);
  this._interval_id = null;
};

// Attempts to match a newly-arrived event. Returns
// a promise that resolves with the match token and data pool or
// with an error.
Matcher.prototype.match = function(event_time, longitude, latitude, data_item) {

  var deferred = Q.defer();

  // Fail immediately if the queue is full
  if (this._event_queue.length >= this._MAX_QUEUE_SIZE) {
    deferred.reject('queue_full');
    return deferred.promise;
  }

  // Enqueue the event details
  this._event_queue.push({
    request_time: Date.now(),
    event_time: event_time,
    location: { 
      longitude: longitude,
      latitude: latitude,
    },
    data_item: data_item,
    deferred: deferred
  });

  // If the matcher is not running, start it
  this.start();

  return deferred.promise;
}

// Internal method that processes a set of events
// that are at least _MAX_REQUEST_SKEW old and tries
// to match them.
Matcher.prototype._process_batch = function () {
  var cutoff = Date.now() - this._MAX_REQUEST_SKEW;

  var i;
  for (
    i = 0; 
    (i < this._event_queue.length) && (this._event_queue[i].request_time < cutoff);
    i ++
  ) {
    var target = this._event_queue[i];
    if (target.matched) continue;

    var matched_events = [target];
    for (var j = i + 1; j < this._event_queue.length; j ++) {
      var other = this._event_queue[j];
      if (
        !other.matched &&
        (Math.abs(target.event_time - other.event_time) <= this._MAX_CLOCK_SKEW) &&
        (geolib.getDistance(target.location, other.location) < this._MAX_DISTANCE)
      ) {
        target.matched = true;
        other.matched = true;
        matched_events.push(other);
      }
    }

    if (target.matched) {
      // Resolve promises for all the matches against this target
      var token = uuid.v4();
      var data_pool = [];
      matched_events.forEach(function(e) { 
        if (e.data_item !== undefined) data_pool.push(e.data_item);
      });

      matched_events.forEach(function(e) {
        e.deferred.resolve({token: token, data: data_pool});
      });
    } else {
      // If we never found a match, raise an error to the target's promise
      target.deferred.reject('no_match');
    }
  }

  // Remove all the target events we've considered so far from the queue
  this._event_queue.splice(0, i);
};
