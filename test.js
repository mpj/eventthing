var EventThing = require('./index')
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var deepMatches = require('mout/object/deepMatches')
var find = require('mout/array/find')
var remove = require('mout/array/remove')
var isArray = require('mout/lang/isArray')
var partial = require('mout/function/partial')
var _ = require('highland')
var Q = require('q')

describe('', function() {

  var db;
  var thing;
  beforeEach(function(done) {
    var url = 'mongodb://localhost:27017/test-'+Math.floor(Math.random()*100000);
    MongoClient.connect(url, function(err, x) {
      db = x;
      thing = EventThing(db)
      done()
    });
  })

  afterEach(function(done) {
    db.dropDatabase();
    db.close();
    done()
  })


  it('streams', function(done) {
    _(thing.subscribe({}))
      .through(await({ hello: 'world'}))
      .pull(done)

    thing.push({
      hello: 'world'
    })
  })

  it('allows streaming written stuff', function(done) {
    thing.push({ hello: 0 });
    thing.push({ hello: 1 });
    thing.push({ hello: 2 });
    _(thing.subscribe({})).through(await([
      { hello: 0 },
      { hello: 1 },
      { hello: 2 }
    ])).pull(done);
  })

  it('allows streaming with offset', function(done) {
    thing.push({ hello: 0 });
    thing.push({ hello: 1 });
    thing.push({ hello: 2 });
    _(thing.subscribe({ offset: 1})).through(await.not([
      { hello: 0 }
    ])).each(function() {})
    setTimeout(done, 100)
  })


  var awaitGeneric = _.curry(function await(expect, patterns, stream) {
    if (!isArray(patterns)) patterns = [ patterns ];
    return stream.consume(function (err, x, push, next) {
      if (expect) {
        remove(patterns, find(patterns, function(pattern) {
          return deepMatches(x, pattern)
        }))
      } else {
        var match = find(patterns, function(pattern) {
          return deepMatches(x, pattern);
        });
        if (match)
          throw new Error(
            'Should not have been gotten ' + JSON.stringify(match));
      }

      if (patterns.length === 0) push(null, null);
      next()

    });
  })
  var await = partial(awaitGeneric, true)
  await.not = partial(awaitGeneric, false)




})
