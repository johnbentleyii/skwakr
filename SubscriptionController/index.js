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


    authorization = crypto.createHmac('sha1', 'Squawker').update(event.UserID).digest('base64');
    if( authorization != event.SessionID ) {
        context.fail( "Unauthorized" );
    }
    
    dynamo.getItem( { TableName: "SkwakrUsers", Key: { UserID : event.UserID } }, function( err, users ) {
        subscriber = users.Item;
        console.log( users );
        
    var params = {
        TableName: "SkwakrUsers", 
        FilterExpression: "(Username = :u1)",
        ExpressionAttributeValues: { ":u1": event.payload.Username }
    };
    dynamo.scan( params, function( err, users ) {
        console.log( users );
        console.log( err );
        if( users.Count === 0 ) {
            context.fail( "Username " + event.payload.Username + " was not found." );
        }
        
        for( var i=0; i<users.Count; i++ ) {
            if( users.Items[i].Username == event.payload.Username ) {
                publisher = users.Items[i];
                break;
            }
        }

    switch (operation) {
        case 'subscribe':
            var sns = new aws.SNS();
            sns.createTopic( { "Name": publisher.UserID }, function( err, topicData ) {
                if( err ) {
                    context.fail( "Unable to publish message.");
                }
                if( topicData ) {
                    var topicArn = topicData.TopicArn;
                    var sqs = new aws.SQS();
                    sqs.createQueue( {
                        QueueName: event.UserID,
                        Attributes: {
                            Policy: '{ "Version": "2008-10-17", "Id": "Skwawkr-SQS", "Statement": [ { "Sid": "Skawkr", "Effect": "Allow", "Principal": "*", "Action": "SQS:*" } ] }'
                        }
                        }, function( err, data ) {
                            if( err ) {
                                console.log(err);
                                context.fail( "Unable to subscribe to " + publisher.Username + " at this time." );
                            }
                            if( data ) {
                                console.log( data );
                                sqs.getQueueAttributes( { QueueUrl: data.QueueUrl, AttributeNames: [ 'QueueArn' ] }, function( e, sqsAttribs ){
                                    console.log( e );
                                    console.log( sqsAttribs );
                                    sns.subscribe( {
                                        TopicArn: topicArn,
                                        Protocol: 'sqs',
                                        Endpoint: sqsAttribs.Attributes.QueueArn,
                                    }, function( e, d ) { console.log( e ); console.log( d ); } );
                                } );
                                if( event.payload.SendEmail ) {
                                    sns.subscribe( {
                                        TopicArn: topicArn,
                                        Protocol: 'email',
                                        Endpoint: subscriber.Email
                                    }, function( e, d ) { console.log( e ); console.log( d ); } );  
                                }
                                if( event.payload.SendSMS ) {
                                    sns.subscribe( {
                                        TopicArn: topicArn,
                                        Protocol: 'sms',
                                        Endpoint: subscriber.Mobile
                                    }, function( e, d ) { console.log( e ); console.log( d ); } ); 
                                }
                            }
                        }
                    );
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
})})}