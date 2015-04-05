var Q = require('q')
var _ = require('highland')
var prop = require('mout/function/prop')
var inspector = function (name) {
  name = name || 'inspectooor'
  return function(x) {
    console.log('inspecting', name+':', x)
    return x;
  }
}
var argInspector = function() {
  console.log('argInspector', arguments)
}

var EventThing = function(db) {

  var state = {}

  function getNextSequence(name) {
    return Q.ninvoke(db.collection('counters'), 'findAndModify',
      { '_id': name },
      null,
      { '$inc': { seq: 1 } },
      {new: true}
    ).then(function(doc) {
      return doc.value.seq;
    })
  }

  function getCollection() {
    if (!state.whenCollection) {
      var deferred = Q.defer();
      db.createCollection('eventlog', {strict: true}, function(error, result) {
        if (!!error) {
          var alreadyExists =
            error.code === 48 ||
            error.message === 'Collection eventlog already exists. Currently in strict mode.';
          if (alreadyExists) {
            deferred.resolve();
          } else {
            throw error;
          }
          // already created
        } else {

          var whenDispatch =
            Q.ninvoke(
              db,
              'createCollection',
              'eventdispatch',
              { capped: true, size: 100000, strict: true });

          var whenInitialEvent = Q.ninvoke(db.collection('eventlog'), 'insert', {
            initialEvent: true,
            _id: 0
          });
          var whenOrdinalCounter = Q.ninvoke(db.collection('counters'), 'insert', {
            _id: "eventlog-ordinal",
            seq: 0
          });

          Q.all([whenDispatch, whenInitialEvent, whenOrdinalCounter]).then(function() {
            syncToDispatch()
            setTimeout(function() {
              deferred.resolve()
            }, 100)

          })
          .done()
        }
      })
      state.whenCollection = deferred.promise;
    }

    return state.whenCollection.then(function(){
      return db.collection('eventlog')
    });
  }

  function syncToDispatch() {
    db
      .collection('eventdispatch')
      .find({})
      .sort({ $natural: -1})
      .limit(1)
      .nextObject(function(err, latest) {
        var latestDispatchedOrdinal = !!latest ? latest._id : -1;
        var filter = { _id : { '$gt': latestDispatchedOrdinal } };
        var str = db.collection('eventlog').find(filter).sort({ _id: 1}).stream();
        str.resume();
        _(str).each(function(x) {
          db.collection('eventdispatch').insert(x)
        })
      })
  }

  return {
    subscribe: function(opts) {
      opts = opts || {}
      opts.conditions = opts.conditions || {}

      var out = _()
      getCollection().then(function(coll) {

        var filter = {}
        if (!!opts.offset)
          filter._id = { '$gte': opts.offset };


        var bodyStream = coll.find(filter)
          .sort({ _id: 1})
          .stream();
        bodyStream.resume();
        var lastEnvelope = null;
        var chainIsBroken = false;
        _(bodyStream).filter(function(x) {
          chainIsBroken =
            chainIsBroken ||
            (lastEnvelope !== null &&
            lastEnvelope._id !== x._id-1);
          if (!chainIsBroken) {
            lastEnvelope = x;
            return true;
          } else {
            return false
          }
        }).on('data', function(d) {
          out.write(d)
        })



        out.observe().each(function(x) {
          lastEnvelope = x;
        })

        bodyStream.on('end', function(x) {

          if (!!lastEnvelope)
            filter._id =  {$gt: lastEnvelope._id};
          var options = {
            tailable: true,
            awaitdata: true,
            numberOfRetries: -1
          }
          var tailStream = db.collection('eventdispatch')
            .find(filter, options)
            .stream()
          tailStream.resume();
          tailStream.pipe(out);

        })

      }).done()
      return out.map(prop('body'));

    },
    push: function(evt) {
      return getCollection()
        .then(function(coll) {
          return getNextSequence('eventlog-ordinal')
            .then(function(ordinal) {
              var deferred = Q.defer();
              coll.insert({
                _id: ordinal,
                body: evt
              }, deferred.makeNodeResolver())
              return  deferred.promise;
            })
            .then(function() {
              syncToDispatch();
            })
        })

    }
  }
}
module.exports = EventThing
