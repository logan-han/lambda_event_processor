var AWS = require("aws-sdk");
var doc = require('dynamodb-doc');
var dynamodb = new doc.DynamoDB();
var https = require('https');
var queryString = require('querystring');
var TASK_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/blah";
var AWS_REGION = "us-west-2";

var sqs = new AWS.SQS({region: AWS_REGION});
var s3 = new AWS.S3({region: AWS_REGION});

function deleteMessage(receiptHandle, cb) {
  sqs.deleteMessage({
    ReceiptHandle: receiptHandle,
    QueueUrl: TASK_QUEUE_URL
  }, cb);
}

function work(task, cb) {
  console.log(JSON.stringify(task, null, '  '));
  var tableName = "INSERT_TABLE_NAME";
  task = JSON.parse(task);
  var msg_id=task.Id;
  var total_parts=task.TotalParts;
  var part_number=task.PartNumber;
  var part_data=task.Data;
  var payload="";

  var params = {
    TableName : tableName,
    KeyConditionExpression: "#msg_id = :msg_id",
    ExpressionAttributeNames:{
        "#msg_id": "msg_id"
    },
    ExpressionAttributeValues: {
        ":msg_id":msg_id
    }
  }

  dynamodb.query(params, function(err, data){
      if (err)
      {
          console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
      }
      else
      {
          console.log("Query succeeded.");
          if (data.Items.length > 0)
              {
                if (data.Items[0].total_parts - data.Items.length === 1)
                {
                    data.Items.forEach(function(item) {
                      payload = item.part_data;
                    })
                // poor hack here
                if (part_number == "0")
                {
                  payload = part_data + payload;
                }
                else
                {
                  payload = payload + part_data;
                }
                  var options = {
                        host: 'HTTP_ENDPOINT',
                        port: 443,
                        path: '/HTTP_PATH/' + msg_id,
                        method: 'POST',
                        headers: {
                          'x-token': 'blah'
                        }
                      };
                    var req = https.request(options, function (res) {
                      res.setEncoding('utf-8');
                      var responseString = '';
                      res.on('data', function (data) {
                        responseString += data;
                      });
                      res.on('end', function () {
                        console.log('Response: ' + responseString);
                        });
                     });
                     req.on('error', function (e) {
                       console.error('HTTP error: ' + e.message);
                     });
                     console.log('API call: ' + payload);
                     req.write(payload);
                     req.end();
                }
              }
          else
              {
                dynamodb.putItem({
                  "TableName": tableName,
                  "Item" : {
                    "msg_id": msg_id,
                    "total_parts": total_parts,
                    "part_number": part_number,
                    "part_data": part_data,
                  }
                }, function(err, data) {
                    if (err) {
                        cb();
                    }
                    else {
                        console.log('great success: %j',data);
                        cb();
                    }
                });
              }
      }
  })

  cb();
}

exports.handler = function(event, context, callback) {
  work(event.Body, function(err) {
    if (err) {
      deleteMessage(event.ReceiptHandle, callback);
    } else {
      deleteMessage(event.ReceiptHandle, callback);
    }
  });
};
