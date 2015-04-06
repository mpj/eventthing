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

  function syncToDispatch() {
    return Q.ninvoke(
      db
        .collection('eventdispatch')
        .find({})
        .sort({ $natural: -1})
        .limit(1), 'nextObject')
    .then(function(latest) {
      var latestDispatchedOrdinal = !!latest ? latest._id : -1;
      var filter = { _id : { '$gt': latestDispatchedOrdinal } };
      return Q.ninvoke(
        db.collection('eventlog').find(filter).sort({ _id: 1}), 'toArray')
          .then(function(result) {
            // Create an unbroken chain, throw away items after gap
            // i.e. 0,1,2,4,5 becomes 0,1,2
            return result.reduce(function(prev, cur, index, arr) {
              if(prev.length === 0 || prev[prev.length-1]._id === cur._id-1)
                prev.push(cur);
              return prev;
            }, []);
          });
    })
    .then(function(loggedEventsToDispatch) {
      // Return if work needs to be done
      if (loggedEventsToDispatch.length === 0) return true;

      return Q.ninvoke(
        db.collection('eventdispatch'),
        'insert',
        loggedEventsToDispatch,
        { ordered: true }
      )
      .fail(function(error) {
        var isDupe = error.code === 11000;
        if (isDupe) return true;
        throw error;
      })
    }).then(function() {
      return true
    })
  }

  return {
    subscribe: function(opts) {
      opts = opts || {}
      opts.conditions = opts.conditions || {}

      var out = _()
      var lastEnvelopeOut = null;
      out.observe().each(function(x) { lastEnvelopeOut = x; })

      // outWithBrokenChainFilter - astream that writes to out,
      // but that first filters out items that would break the chain.
      // I.e. if an event with an _id of 4 has just been written,
      // 5 can be written afterwards, but 6 would be filtered out unless
      // was written first.
      var outWithBrokenChainFilter = (function() {
        var strm = _()
        var chainIsBroken = false;
        strm
          .filter(function(x) {
            chainIsBroken =
              chainIsBroken || (
                lastEnvelopeOut     !== null &&
                lastEnvelopeOut._id !== x._id - 1
              );
            return !chainIsBroken;
          })
          .pipe(out)
        return strm;
      })();

      // Tailing for a query that returns 0
      // will result in a dead cursor - instead, we use a $gte instead
      // of $gt which will create tail that will return the doc we
      // know exists, i.e. the lastEnvelopeOut, and throw it away
      var outWithDupeFilter = (function() {
        var strm = _();
        strm.filter(function(x) {
          return x._id !== lastEnvelopeOut._id;
        }).pipe(out);
        return strm;
      })();

      getCollection().then(function(coll) {

        var filter = {}
        if (!!opts.offset)
          filter._id = { '$gte': opts.offset };

        var bodyStream = coll.find(filter)
          .sort({ _id: 1})
          .stream();
        bodyStream.pipe(outWithBrokenChainFilter, { end: false });
        bodyStream.on('end', function() { startTailing() });

        function startTailing() {
          if (!!lastEnvelopeOut)
            filter._id =  { '$gte': lastEnvelopeOut._id };

          var tailStream =
            db.collection('eventdispatch')
              .find(filter, {
                tailable: true,
                awaitdata: true,
                numberOfRetries: -1
              })
              .stream();

          tailStream.resume(); // Resume is needed for the tailable cursor, not
                               // completely sure why.
          tailStream.pipe(outWithDupeFilter, { end: false });
          tailStream.on('end', function() {
            // A tail might end pretty fast if there are no records,
            // so a re-query immidieately might have quite a toll on
            // the system. Resume after 0-1000ms.
            setTimeout(startTailing, Math.floor(Math.random()*1000));
          })
        }
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
