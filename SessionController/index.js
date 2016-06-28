console.log('Loading function');

var doc = require('dynamodb-doc');
var dynamo = new doc.DynamoDB();

var crypto = require('crypto');

/**
 * Provide an event that contains the following keys:
 *
 *   - operation: one of the operations in the switch statement below
 *   - payload: a parameter to pass to the operation being performed
 */
exports.handler = function(event, context) {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    var operation = event.operation;

    switch (operation) {
        case 'login':
            var query = { 
                "TableName" : "SkwakrUsers",
                "FilterExpression" : "Username = :u1",
                "ExpressionAttributeValues": {
                    ":u1" : event.payload.username
                }
            };
            dynamo.scan( query, function( err, data ) {
                console.log( err );
                console.log( data );
                if(err || (data.Count === 0) || (data.Items[0].Password != event.payload.password)) {
                  context.fail( new Error( 'Invalid username or password') );                  
                } else {
                    sessionId = crypto.createHmac('sha1', 'Squawker').update(data.Items[0].UserID).digest('base64');
                    context.succeed( { "SessionID" : sessionId, "UserID": data.Items[0].UserID } );
                }
            } );
            break;
        default:
            context.fail(new Error('Unrecognized operation "' + operation + '"'));
    }
};