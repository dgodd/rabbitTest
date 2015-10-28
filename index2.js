var amqp = require('amqp');
var conn = amqp.createConnection({url: "amqp://guest:guest@localhost:5672"});

var MongoClient = require('mongodb').MongoClient;
// Wait for connection to become established.
conn.on('ready', function () {
  // Use the default 'amq.topic' exchange
  conn.queue('dave', {autoDelete: false}, function (q) {
    console.log('Queue ' + q.name + ' is open');

    // Catch all messages
    q.bind('#');

    var url = 'mongodb://localhost:27017/myproject';
    MongoClient.connect(url, function(err, db) {
      if(err) {
        console.log(err);
        return;
      }
      console.log("Connected correctly to server");
      var collection = db.collection('documents');

      // Receive messages
      q.subscribe({ ack: true, prefetchCount: 1 }, function (message, headers, deliveryInfo, messageObject) {
        console.log('Got a message with routing key ' + deliveryInfo.routingKey + ' ::: ' + message.hi);
        collection.insertMany([ message ], function(err, result) {
          if(err) {
            console.log(err);
          } else {
            messageObject.acknowledge(false);
          }
        });
      });
    });
  });
});
conn.on('error', function(e) {
  console.log('onError: ' + e);
});
conn.on('close', function(e) {
  console.log('onClose: ' + e);
});

conn.on('ready', function () {
  var exc = conn.exchange('amq.topic');
  var i = 0;
  setInterval(function() { 
    i += 1;
    exc.publish('jimbo', {hi:'mom' + i});
  }, 1);
});
