var Q = require('q')
var _ = require('highland')
var prop = require('mout/function/prop')
var debounce = require('just-debounce')
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

  function ensureCollection(name, opts) {
    opts = opts || {}
    var deferred = Q.defer();
    db.createCollection(name, opts, function(err) {
      if (err)
        deferred.reject(err);
      else
        deferred.resolve(db.collection(name));
    });
    return deferred.promise;
  }

  function insert(collectionName, document) {
    var deferred = Q.defer();
    db.collection(collectionName).insert(document, deferred.makeNodeResolver())
    return deferred.promise;
  }

  function getCollection() {
    if(!state.whenCollection) {

      var deferred = Q.defer();

      var eventLogPromise =
        ensureCollection('eventlog')
          .then(function() {
            return insert('eventlog', {
              body: {initialEvent: true},
              _id: 0
            })
          }).fail(function (error) {
            var isDupe = error.code === 11000;
            if (isDupe) return true;
            throw error;
          })
      var eventDispatchPromise =
        ensureCollection('eventdispatch', { capped: true, size: 100000 })
          .then(function() {
            return insert('eventdispatch', {
              body: {initialEvent: true},
              _id: 0
            })
          })
          .fail(function (error) {
            var isDupe = error.code === 11000;
            if (isDupe) return true;
            throw error;
          })
      var whenOrdinalCounter = insert('counters', {
        _id: "eventlog-ordinal",
        seq: 0
      }).fail(function (error) {
        var isDupe = error.code === 11000;
        if (isDupe) return true;
        throw error;
      });
      Q.all([eventLogPromise, eventDispatchPromise,whenOrdinalCounter]).then(function() {
        deferred.resolve()
      }).done()
      state.whenCollection = deferred.promise;
    }

    return state.whenCollection.then(function(){
      return db.collection('eventlog')
    });
  }

  var total = 0;
  function syncToDispatch() {
    var deferred = Q.defer();

    db
      .collection('eventdispatch')
      .find({})
      .sort({ $natural: -1})
      .limit(1)
      .nextObject(function(err, latest) {
        var latestDispatchedOrdinal = !!latest ? latest._id : -1;
        var filter = { _id : { '$gt': latestDispatchedOrdinal } };
        db.collection('eventlog').find(filter).sort({ _id: 1}).toArray(function(err, result) {
          if (err) return deferred.reject(err);
          result = result.reduce(function(prev, cur, index, arr) {
            if(prev.length === 0 || prev[prev.length-1]._id === cur._id-1)
              prev.push(cur);
            return prev;
          },[]);

          if (result.length > 0) {
            db
              .collection('eventdispatch')
              .find({})
              .sort({ $natural: -1})
              .limit(1)
              .nextObject(function(err, latest) {
                db.collection('eventdispatch').insert(result, {ordered: true}, function() {
                  deferred.resolve()
                });
            })
          } else {
            deferred.resolve()
          }
        })
      })
    return deferred.promise;
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

        function startTailing() {
          if (!!lastEnvelope)
            filter._id =  {$gte: lastEnvelope._id};
          var options = {
            tailable: true,
            awaitdata: true,
            numberOfRetries: -1
          }
          var tailStream = db.collection('eventdispatch')
            .find(filter, options)
            .stream()

          tailStream.resume();
          tailStream.on('end', function() {
            // A tail might end pretty fast if there are no records,
            // so a re-query immidieately might have quite a toll on
            // the system. Resume after 0-1000ms.
            setTimeout(startTailing, Math.floor(Math.random()*1000));
          })

          _(tailStream)
            .filter(function(x) {
              // Tailing for a query that returns 0
              // will result in a dead cursor - instead, we use a $gte instead
              // of $gt which will create tail that will return the doc we
              // know exists, i.e. the lastEnvelope, and throw it away
              return x._id !== lastEnvelope._id;
            }).on('data', function(x) {
              out.write(x)
            })
        }

        bodyStream.on('end', function() {
          startTailing()
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
              return syncToDispatch();
            })
        })

    }
  }
}
module.exports = EventThing
