var Q = require('q')
var _ = require('highland')
var prop = require('mout/function/prop')
var EventThing = function(db) {

  var state = {}

  function getNextSequence(name) {
    return Q.ninvoke(db.collection('counters'), 'findAndModify',
      { '_id': name },
      null,
      { '$inc': { seq: 1 } },
      {new: true}
    ).then(function(doc) {
      return doc.seq;
    })
  }

  function getCollection() {
    if (!state.whenCollection) {
      var deferred = Q.defer();
      db.createCollection('eventlog', { capped: true, size: 100000, strict: true }, function(err, result) {
        if (!err) {
          var whenInitialEvent = Q.ninvoke(db.collection('eventlog'), 'insert', {
            initialEvent: true
          });
          var whenOrdinalCounter = Q.ninvoke(db.collection('counters'), 'insert', {
            _id: "eventlog-ordinal",
            seq: 0
          });

          Q.all([whenInitialEvent, whenOrdinalCounter]).then(function() {
            deferred.resolve()
          }).fail(function(err) {
            console.log(err)
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
      opts.conditions = opts.conditions || {}

      var out = _()
      getCollection().then(function(coll) {
        var options = {
          tailable: true,
          awaitdata: true,
          numberOfRetries: -1
        }
        var filter = {}
        if (!!opts.offset)
          filter._id = { '$gte': opts.offset };
        var str = coll
          .find(filter, options)
          .stream()
        str.resume() // force stream to old mode
        str.pipe(out);
      })
      return out.map(prop('body'));

    },
    push: function(evt) {
      // FIXME: This approach will mean that the natrual and ordinal order
      // of events will diverge, and that item 5 might be inserted before item 4.
      // We have to make sure that the subscribe stream compensates for this
      getCollection().then(function(coll) {
        getNextSequence('eventlog-ordinal').then(function(ordinal) {
          coll.insert({
            _id: ordinal,
            body: evt
          });
        })

      }).done()
    }
  }
}
module.exports = EventThing
