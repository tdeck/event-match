var Matcher = require('../matcher').Matcher,
    chai = require('chai'),
    Q = require('q'),
    expect = chai.expect;

chai.use(require('chai-subset'));
chai.use(require('chai-as-promised'));

describe('Event matcher', function() {
  var matcher;
  beforeEach(function() { 
    // Stop any running interval batch so we don't leak memory
    if (matcher) matcher.stop();

    matcher = new Matcher({
      interval: 5, 
      max_request_skew: 10,
      max_queue_size: 4,
      max_distance: 100,
      max_clock_skew: 5
    });
  });

  it('lets you enqueue an event', function() {
    matcher.match(12345, 678, 90, 'Data');

    // Check that the matcher has started
    expect(matcher._interval_id).to.exist;

    // Check that the event is there
    expect(matcher._event_queue.length).to.equal(1);

    var pushed_event = matcher._event_queue[0];
    expect(pushed_event.request_time).to.exist;
    expect(pushed_event.deferred).to.exist;
    expect(pushed_event).to.containSubset({
      event_time: 12345,
      location: {
        longitude: 678,
        latitude: 90,
      },
      data_item: 'Data'
    });
  });

  it('caps the queue size', function(done) {
    expect(Q.all([
      expect(matcher.match(12, 34, 56, 'X')).to.be.fulfilled,
      expect(matcher.match(12, 34, 56, 'X')).to.be.fulfilled,
      expect(matcher.match(12, 34, 56, 'X')).to.be.fulfilled,
      expect(matcher.match(12, 34, 56, 'X')).to.be.fulfilled,
      expect(matcher.match(12, 34, 56, 'Y')).to.be.rejectedWith('queue_full'),
    ])).to.notify(done);
  });

  it('matches an event with itself', function(done) {
    Q.all([
      matcher.match(12, 34, 56, 'A'),
      matcher.match(12, 34, 56, 'B'),
      matcher.match(12, 34, 56, 'C'),
    ]).spread(function(matchA, matchB, matchC) {
      // Check that they all have the same token
      expect(matchA.token).to.exist;
      expect(matchA.token).to.equal(matchB.token);
      expect(matchB.token).to.equal(matchC.token);

      // Check the data pool is the same
      expect(matchA.data).to.eql(['A', 'B', 'C']);
      expect(matchB.data).to.eql(['A', 'B', 'C']);
      expect(matchC.data).to.eql(['A', 'B', 'C']);
      done();
    }).catch(done).done();
  });

  it('matches two events with times < max_clock_skew', function(done) {
    expect(Q.all([
      expect(matcher.match(0, 34, 56, 'X')).to.be.fulfilled,
      expect(matcher.match(5, 34, 56, 'X')).to.be.fulfilled,
    ])).to.notify(done);
  });

  it('does not match events with times > max_clock_skew', function(done) {
    expect(Q.all([
      expect(matcher.match(0, 34, 56, 'X')).to.be.rejectedWith('no_match'),
      expect(matcher.match(6, 34, 56, 'X')).to.be.rejectedWith('no_match'),
    ])).to.notify(done);
  });

  it('matches two events within max_distance', function(done) {
    expect(Q.all([
      expect(matcher.match(0, 37.775206, -122.418208)).to.be.fulfilled,
      expect(matcher.match(0, 37.775206, -122.419044)).to.be.fulfilled,
    ])).to.notify(done);
  });

  it('does not match events beyond max_distance', function(done) {
    expect(Q.all([
      expect(matcher.match(0, 37.775206, -122.418208)).to.be.rejectedWith('no_match'),
      expect(matcher.match(0, 37.776322, -122.419430)).to.be.rejectedWith('no_match'),
    ])).to.notify(done);
  });

  it('partitions interleaved matches properly', function(done) {
    Q.all([
      matcher.match(0, 37.000000, -122.000000),
      matcher.match(8, 37.000000, -122.000000),
      matcher.match(2, 37.000123, -122.000123),
      matcher.match(6, 37.000001, -122.000002),
    ]).spread(function(matchA, matchB, matchC, matchD) {
      expect(matchA.token).to.equal(matchC.token);
      expect(matchB.token).to.equal(matchD.token);
      expect(matchA.token).to.not.equal(matchB.token);
      done();
    }).catch(done).done();
  });

  it('respects max_request_skew', function(done) {
    var promiseA = matcher.match(0, 0, 0);
    setTimeout(function() {
      var promiseB = matcher.match(0, 0, 0);

      expect(Q.all([
        expect(promiseA).to.be.rejectedWith('no_match'),
        expect(promiseB).to.be.rejectedWith('no_match'),
      ])).to.notify(done);
    }, 20);
  });
});
