var Q = require('q')
var _ = require('highland')
var prop = require('mout/function/prop')
var range = require('mout/array/range')

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

  function throwUnlessDupe(error) {
    var isDupe = error.code === 11000;
    if (isDupe) { return true; }
    throw error;
  }

  var state = {}

  function populatePlaceholders(max) {
    return Q.ninvoke(db
        .collection('eventlog'),
        'count',
        { body: { $exists: false } }
      )
      .then(function(placeHolderCount) {
        if (placeHolderCount > 50000) return;
        return Q.ninvoke(db
          .collection('eventlog')
          .find({})
          .sort({ _id: -1 })
          .limit(1), 'nextObject')
          .then(function(latestPlaceHolder) {

            var latestLoggedOrdinal = !!latestPlaceHolder ? latestPlaceHolder._id : -1;
            var docs = range(1, max ? 999 : 10).map(function(i) {
              return { _id: latestLoggedOrdinal + i }
            })

            return Q.ninvoke(db.collection('eventlog'), 'insert', docs, { ordered: true } )
              .fail(throwUnlessDupe)
              .then(function(res) {
                return true;
              });
          })
      });
  }

  function ensureCollections() {
    if(!state.whenCollections) {

      var eventLogPromise =
        Q.ninvoke(db,'createCollection', 'eventlog')
          .then(function() {
            var deferred = Q.defer();
            db.collection('eventlog').insert({
              body: {initialEvent: true},
              _id: 0
            }, deferred.makeNodeResolver())
            return deferred.promise;
          }).fail(throwUnlessDupe)

      var eventDispatchPromise =
        Q.ninvoke(db, 'createCollection', 'eventdispatch', { capped: true, size: 100000 })
          .then(function() {
            var deferred = Q.defer();
            db.collection('eventlog').insert({
              body: {initialEvent: true},
              _id: 0
            }, deferred.makeNodeResolver())
            return deferred.promise;
          })
          .fail(throwUnlessDupe)

      state.whenCollections =
        Q.all([eventLogPromise, eventDispatchPromise])
          .then(populatePlaceholders)
          .then(function() {
            return true;
          })

    }

    return state.whenCollections;
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
      var filter = {
        _id : { '$gt': latestDispatchedOrdinal },
        body: { '$exists': true }
      };
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
      .fail(throwUnlessDupe)
    })
    .then(function() {
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

      // outWithBrokenChainFilter - a stream that writes to out,
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

      // outWithDupeFilter - a stream that writes to out, but will filter out
      // any events that is a dupe of the last envelope out.
      // This exists because tailing for a query that returns 0
      // will result in a dead cursor - instead, the tailing cursor uses a
      // $gte instead of $gt which will create tail that will include the doc we
      // know exists, i.e. the lastEnvelopeOut, and then we use
      // outWithDupeFilter to just throw it away.
      var outWithDupeFilter = (function() {
        var strm = _();
        strm.filter(function(x) {
          return x._id !== lastEnvelopeOut._id;
        }).pipe(out);
        return strm;
      })();

      ensureCollections().then(function(coll) {

        var filter = {
          body: { '$exists': true }
        }
        if (!!opts.offset)
          filter._id = { '$gte': opts.offset };

        var bodyStream = db.collection('eventlog').find(filter)
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
      return ensureCollections()
        .then(function() {
          return Q.ninvoke(db.collection('eventlog'), 'findAndModify',
            { body: { $exists: false } },
            [['_id', 1]],
            { $set: {body: evt} },
            { w: 1, j: 1, wtimeout: 5000, new:true }
          )
        })
        .then(function(res) {
          if (!res.value) {
            throw new Error('Could not insert value at pos')
          }
        })
        .then(syncToDispatch)
        .then(populatePlaceholders)
        .then(function() {
          return true
        })
    }
  }
}
module.exports = EventThing
