var doc = require('dynamodb-doc');
var dynamodb = new doc.DynamoDB();
var https = require('https');
var queryString = require('querystring');

exports.handler = function(event, context)
{
    var encoded = event.records[0].kinesis.data;
    var event = new Buffer(encoded, 'base64');
    event = JSON.parse(event);
    console.log(JSON.stringify(event, null, '  '));
    var tableName = "INSERT_TABLE_NAME";

    var msg_id=event.Id;
    var total_parts=event.TotalParts;
    var part_number=event.PartNumber;
    var part_data=event.Data;
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
                          context.done(err);
                      }
                      else {
                          console.log('great success: %j',data);
                          context.succeed("Record Successfully Inserted");
                      }
                  });
                }
        }
    })
}
