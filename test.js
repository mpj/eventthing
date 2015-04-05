var EventThing = require('./index')
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var deepMatches = require('mout/object/deepMatches')
var find = require('mout/array/find')
var remove = require('mout/array/remove')
var isArray = require('mout/lang/isArray')
var partial = require('mout/function/partial')
var prop = require('mout/function/prop')
var _ = require('highland')
var Q = require('q')
var inspector = function(name, thunk) {
  name = name || 'untitled';
  var thr = _()
  thr[thunk ? 'fork' : 'observe']().each(function(x) {
    console.log("[INSPECTING / " + name + "]", x)
  })
  return thr;
}

Q.longStackSupport = true;

// Mocha swallows errors
if(!process.handlerAdded) {
  process.handlerAdded = true;
  process.on('uncaughtException', function (error) {
    console.log('Uncaught error ----\n', error.message, error.stack,'\n-------------------\n');
  })
}
describe('when we have a database', function() {

  this.timeout(4000);

  var db;
  beforeEach(function(done) {
    var url = 'mongodb://localhost:27017/test-'+Math.floor(Math.random()*10000000);
    MongoClient.connect(url, function(err, x) {
      db = x;
      done()
    });
  })

  afterEach(function() {
    db.dropDatabase();
  })


  it('streams', function(done) {
    var thing = EventThing(db)
    _(thing.subscribe({}))
      .through(await({ hello: 'world'}))
      .pull(done)
    setTimeout(function() {
      console.log("writing");
      thing.push({
        hello: 'world'
      }).done()
    }, 100)

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
    setTimeout(done, 250)
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
      Q.ninvoke(db.collection('counters'), 'insert', {
        _id: "eventlog-ordinal",
        seq: 0
      })
      db.collection('eventlog').insert({ _id: 0, body: { hello: 0 } })
      db.collection('eventlog').insert({ _id: 1, body: { hello: 1 } })
      db.collection('eventlog').insert({ _id: 3, body: { hello: 3 } })
      db.collection('eventdispatch').insert({ _id: 0, body: { hello: 0 } })
      db.collection('eventdispatch').insert({ _id: 1, body: { hello: 1 } })
      setTimeout(done, 100)
    })

    it('streams the unbroken block', function(done) {
      var thing = EventThing(db)
      _(thing.subscribe())
        .through(await([{ hello: 0}, {hello:1}])).pull(done)
    })

    it('does not stream past space', function(done) {
      var thing = EventThing(db)
      _(thing.subscribe()).through(await.not({ hello: 3})).each(function(){})
      setTimeout(done, 100)
    })

    it('but when it is inserted, and another event comes in', function(done) {
      var thing = EventThing(db)
      _(thing.subscribe()).through(await([{ hello: 2}, {hello: 3},{hello: 4}])).each(function(){})
      thing.push({ hello: 4})
      setTimeout(done, 100)
    })
  })

  describe('items out of order is in eventlog collection', function() {
    beforeEach(function(done) {
      Q.ninvoke(db.collection('counters'), 'insert', {
        _id: "eventlog-ordinal",
        seq: 0
      })
      db.collection('eventlog').insert({ _id: 1, body: { hello: 1 } })
      db.collection('eventlog').insert({ _id: 0, body: { hello: 0 } })
      db.collection('eventlog').insert({ _id: 3, body: { hello: 3 } })
      db.collection('eventlog').insert({ _id: 2, body: { hello: 2 } })
      db.collection('eventdispatch').insert({ _id: 0, body: { hello: 0 } })
      setTimeout(done, 250)
    })

    it('should return in order', function(done) {
      var thing = EventThing(db)
      _(thing.subscribe())
        .batch(4)
        //.pipe(inspector('what', true))
        .each(function(x) {
          assert.deepEqual(x.map(prop('hello')),
            [0,1,2,3])
          done()
        })
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
