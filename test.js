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
var inspector = function(x) {
  console.log('inspecting:', x)
  return x;
}

Q.longStackSupport = true;

// Mocha swallows errors
/*
process.on('uncaughtException', function (error) {
  console.log('Uncaught error ----\n', error.stack,'\n-------------------\n');
})*/

describe('when we have a database', function() {
  var db;
  beforeEach(function(done) {
    var url = 'mongodb://localhost:27017/test-'+Math.floor(Math.random()*100000);
    MongoClient.connect(url, function(err, x) {
      db = x;
      done()
    });
  })

  afterEach(function(done) {
    db.dropDatabase();
    done()
  })


  it('streams', function(done) {
    var thing = EventThing(db)
    _(thing.subscribe({}))
      .through(await({ hello: 'world'}))
      .pull(done)
    setTimeout(function() {
      thing.push({
        hello: 'world'
      })
    }, 200)

  })

  it('allows streaming written stuff', function(done) {
    var thing = EventThing(db)
    Q.all([
      thing.push({ hello: 0 }),
      thing.push({ hello: 1 }),
      thing.push({ hello: 2 }),
    ]).then(function() {
      _(thing.subscribe({})).through(await([
        { hello: 0 },
        { hello: 1 },
        { hello: 2 }
      ])).pull(done);
    }).done()

  })

  it('allows streaming with offset', function(done) {
    var thing = EventThing(db)
    Q.all([
      thing.push({ hello: 1 }), // Remember dummy event
      thing.push({ hello: 2 }),
      thing.push({ hello: 3 })
    ]).then(function() {
      _(thing.subscribe({ offset: 1 })).through(await.not([
        { hello: 0 }
      ])).each(function() {})
    })
    setTimeout(done, 1000)
  })

  it('retains state', function(done) {
    var thing = EventThing(db)
    thing.push({ hello: 0 }).then(function() {
      var thing2 = EventThing(db);
      _(thing2.subscribe({})).through(await([
        { hello: 0 }
      ])).pull(done);
    })

  })


  describe('items with space is in the event-log', function() {
    beforeEach(function(done) {
      db.collection('eventlog').insert({ _id: 0, body: { hello: 0 } })
      db.collection('eventlog').insert({ _id: 1, body: { hello: 1 } })
      db.collection('eventlog').insert({ _id: 3, body: { hello: 3 } })
      setTimeout(done, 50)
    })

    it('streams the unbroken block', function(done) {
      var thing = EventThing(db)
      _(thing.subscribe())
        .through(await([{ hello: 0}, {hello:1}])).pull(done)
    })

    it('does not stream past space', function(done) {
      var thing = EventThing(db)
      _(thing.subscribe()).through(await.not({ _id: 3})).pull(function(){})
      setTimeout(done, 100)
    })
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
