var EventThing = require('./index')
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert')
var deepMatches = require('mout/object/deepMatches')
var find = require('mout/array/find')
var range = require('mout/array/range')
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

  this.timeout(8000);

  var db;
  before(function(done) {
    var url = 'mongodb://localhost:27017/test-unit';
    MongoClient.connect(url, function(err, x) {
      db = x;
      done()
    });
  })
  after(function() {
    /*try {db.close(function() {}) }
    catch(err) {}*/
  })

  beforeEach(function(done) {
    db.dropDatabase(function(err) {
      if (err) { throw err;};
      done()
    });
  })

  it('streams', function(done) {
    var thing = EventThing(db)
    _(thing.subscribe({}))
      .through(await({ hello: 'worlds'}))
      .pull(done)
    setTimeout(function() {
      thing.push({
        hello: 'worlds'
      }).done()
    }, 100)
  })

  it('streams (even after long time)', function(done) {
    this.timeout(8000);
    var thing = EventThing(db)
    _(thing.subscribe({}))
      .through(await({ hello: 'world'}))
      .pull(done)
    setTimeout(function() {
      thing.push({
        hello: 'world'
      }).done()
    }, 3000)
  })

  it('streams after long time two times', function (done){
    this.timeout(15000);
    var thing = EventThing(db)
    var arr = []
    _(thing.subscribe({}))
      .each(function (x) {
        arr.push(x)
      })

    setTimeout(function() {
      thing.push({
        hello: 1
      }).done()
    }, 4000)

    setTimeout(function() {
      thing.push({
        hello: 2
      }).done()
    }, 8000)

    setTimeout(function() {
      assert.deepEqual(arr, [
        { initialEvent: true },
        { hello: 1 },
        { hello: 2 },
      ])
      done()
    },12000)
  })

  it('allows streaming written stuff', function(done) {
    var thing = EventThing(db)
    Q.all([
      thing.push({ hello: 0 }),
      thing.push({ hello: 1 }),
      thing.push({ hello: 2 }),
    ]).then(function() {
      _(thing.subscribe({}))
      .through(await([
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
        .each(function(x) {
          assert.deepEqual(x.map(prop('hello')),
            [0,1,2,3])
          done()
        })
    })

  })

  it('performs ok', function(done) {
    this.timeout(50000);
    var thing = EventThing(db)
    thing.push({ whatever: 1}).then(function() {

      var count = 100;
      var received = 0;
      var start = null;
      var insertTime = 0;
      thing.subscribe({}).each(function() {
        received++;
        if (received === count) {
          var end = Number(new Date());
          var totalTime = end-start;
          var msPerMessage = totalTime/count;
          assert(msPerMessage < 30);
          /*
          console.log(
            "Received", count, "messages in",
            totalTime, "ms which means",
            msPerMessage, "ms per message")
*/
          done();
        }
      })

      setTimeout(function() {
        _(range(count)).map(function(i) {
          return { hello: i}
        })
        .each(function (x){
          thing.push(x).then(function() {
            insertTime += (insertEnd-insertStart);
          })
        })
      }, 1000)
    }).done()
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
