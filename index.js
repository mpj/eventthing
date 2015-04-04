var Q = require('q')
var _ = require('highland')
var prop = require('mout/function/prop')
var inspector = function (name) {
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
      db.createCollection('eventlog', { capped: true, size: 100000, strict: true }, function(error, result) {
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
          // TODO: Multiple pieces of code trying to create?
          var whenInitialEvent = Q.ninvoke(db.collection('eventlog'), 'insert', {
            initialEvent: true,
            _id: 0
          });
          var whenOrdinalCounter = Q.ninvoke(db.collection('counters'), 'insert', {
            _id: "eventlog-ordinal",
            seq: 0
          });

          Q.all([whenInitialEvent, whenOrdinalCounter]).then(function() {
            deferred.resolve()
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

  return {
    subscribe: function(opts) {
      opts = opts || {}
      opts.conditions = opts.conditions || {}

      var out = _()
      getCollection().then(function(coll) {

        var filter = {}
        if (!!opts.offset)
          filter._id = { '$gte': opts.offset };

        var bodyStream = coll.find(filter).stream();
        bodyStream.pipe(out, { end: false });
        var lastEnvelope = null;
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
          var tailStream = coll
            .find(filter, options)
            .stream()
          tailStream.resume();
          _(tailStream).pipe(out);

        })

      }).done()
      return out.map(prop('body'));

    },
    push: function(evt) {
      // FIXME: This approach will mean that the natrual and ordinal order
      // of events will diverge, and that item 5 might be inserted before item 4.
      // We have to make sure that the subscribe stream compensates for this
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
            });
        })

    }
  }
}
module.exports = EventThing
