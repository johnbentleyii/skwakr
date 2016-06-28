console.log('Loading function');

var aws = require('aws-sdk');
var lambda = new aws.Lambda({
    region: 'us-east-1'
});

var doc = require('dynamodb-doc');
var dynamo = new doc.DynamoDB();

var crypto = require('crypto');
/**
 * Provide an event that contains the following keys:
 *
 *   - operation: one of the operations in the switch statement below
 *   - payload: a parameter to pass to the operation being performed
 */
exports.handler = function(event, context, callback) {
    console.log('Received event:', JSON.stringify(event, null, 2));

    var operation = event.operation;

    if( operation != 'create' ) {
        authorization = crypto.createHmac('sha1', 'Squawker').update(event.UserID).digest('base64');
        if( authorization != event.SessionID ) {
            context.fail( "Unauthorized" );
        }
    }
    
    switch (operation) {
        case 'create':
            var newUser = {
                    "Username" : event.payload.Username,
                    "Password" : event.payload.Password,
                    "Email" : event.payload.Email,
                    "Mobile" : event.payload.Mobile,
                    "FirstName" : event.payload.FirstName,
                    "LastName" : event.payload.LastName
                };
                console.log( JSON.stringify( newUser, null, 2 ));
            var saveUUIDRecord = {
                FunctionName : 'arn:aws:lambda:us-east-1:507320127120:function:saveUUIDRecord',
                Payload : JSON.stringify( { "tableName" : "SkwakrUsers", "uuidPropertyName" : "UserID", "recordValues" : newUser, }, null, 2  )
            };
                            console.log( JSON.stringify( saveUUIDRecord, null, 2 ));
            lambda.invoke( saveUUIDRecord, function(error, data ) {
                console.log( "Invoke error " + error + " " + JSON.stringify( data, null, 2 ) );
                if( error ) {
                    callback( error );
                }
                if( data.Payload ) {
                    var newUser = JSON.parse( data.Payload );
                    var sns = new aws.SNS();
                    sns.createTopic( { "Name": newUser.UserID }, function( err, data ) {
                        console.log( err );
                        console.log( data );
                        sns.setTopicAttributes( {
                            TopicArn: data.TopicArn,
                            AttributeName: 'DisplayName',
                            AttributeValue: 'Skwakr',
                        }, function(e,d) { console.log( 'Set Topic Attributes' ); console.log(e); console.log(d); });
                    } );
                        callback( null, newUser );
                }
            });
            break;
        case 'query' :
            conditions = [];
            expressions = [];
            query = { "TableName" : "SkwakrUsers",
                "ProjectionExpression" : "Username, FirstName, LastName"
            };
            if( event.payload.Username ) {
                query.KeywordConditionExpression.push( "contains(" + event.payload.Username + ", Contains )");
            }
            if( event.payload.FirstName ) {
                conditions.push( "FirstName = " + event.payload.Firstname );
            }
            if( event.payload.LastName ) {
                conditions.push( "LastName contains ('" + event.payload.LastName + "')");
            }
            if( event.payload.Email ) {
                conditions.push( "Email contains ('" + event.payload.Email + "')");
            }
            if( conditions.length ) {
                query.FilterExpression = '';
                for( i=0; i < (conditions.length - 1); i++ ) {
                    query.FilterExpression += conditions[i] + ' AND '; 
                }
                if( i < conditions.length ) {
                    query.FilterExpression += conditions[i];
                }
                //context.fail( JSON.stringify( query ) );
                dynamo.scan(query, context.done);
            } else {
                dynamo.scan(query, context.done);
            }
 
            break;
        case 'read':
            dynamo.getItem(event.payload, context.done);
            break;
        case 'update':
            dynamo.updateItem(event.payload, context.done);
            break;
        case 'delete':
            dynamo.deleteItem(event.payload, context.done);
            break;
        case 'list':
            dynamo.scan(event.payload, context.done);
            break;
        case 'echo':
            context.succeed(event.payload);
            break;
        case 'ping':
            context.succeed('pong');
            break;
        default:
            context.fail(JSON.stringify(event, null, 2) );
            context.fail(new Error('Unrecognized operation "' + operation + '"'));
    }
};