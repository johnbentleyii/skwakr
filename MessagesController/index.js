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


    switch (operation) {
        case 'publish':
            dynamo.getItem( { TableName: "SkwakrUsers", "Key": { UserID: event.UserID } }, function( err, user ) {
                console.log( user );
                var sns = new aws.SNS();
                sns.createTopic( { "Name": user.Item.UserID }, function( err, data ) {
                    if( err ) {
                        context.fail( "Unable to publish message.");
                    }
                    if( data ) {
                        sns.publish( {
                            TopicArn: data.TopicArn,
                            Message: JSON.stringify(
                                {
                                    "default": "Hello", 
                                    "email": event.payload.Message, 
                                    "sqs": event.payload.Message, 
                                    "sms": event.payload.Subject + " - " + event.payload.Message
                                }),
                            MessageStructure: 'json',
                            Subject: event.payload.Subject,
                            MessageAttributes: {
                                Sender: {
                                    DataType: 'String',
                                    StringValue: user.Item.Username
                                }
                            }
                        }, function(e,d) { console.log(e); console.log(d); } );
                    }
                } );
            });
            break;
        case 'query' :
            var sqs = new aws.SQS();
            sqs.createQueue( {
                    QueueName: event.UserID,
                }, 
                function( err, sqsData ) {
                     if( err ) {
                        context.fail( 'Unable to retrieve messages.');
                    }
                    sqs.receiveMessage( {
                        QueueUrl: sqsData.QueueUrl,
                        MaxNumberOfMessages: 10,
                        AttributeNames: [ 'All' ]
                    }, function( err, msgData ) {
                        if( err ) {
                            context.fail( 'Unable to retrieve messages.');
                        }
                        console.log( msgData );
                        var messages = [];
                        var deleteMessages = [];
                        if( msgData && msgData.Messages ) {
                            for( var i=0; i<msgData.Messages.length; i++ ) {
                                console.log( msgData.Messages[i].Body );
                                var body = JSON.parse( msgData.Messages[i].Body );
                                console.log( msgData.Messages[i].Attributes );
                               var message = {};
                               var deleteMessage = {};
                               deleteMessage.ReceiptHandle = msgData.Messages[i].ReceiptHandle;
                               deleteMessage.Id = message.MessageID = msgData.Messages[i].MessageId;
                               message.Message = body.Message;
                               message.Date = body.Timestamp;
                               message.Subject = body.Subject;
                               message.Sender = msgData.Messages[i].Attributes.Sender;
                               messages.push( message );
                               deleteMessages.push( deleteMessage );
                            } 
                        }
                        console.log( messages );
                        console.log( deleteMessages );

                        sqs.deleteMessageBatch( {
                            QueueUrl: sqsData.QueueUrl,
                            Entries: deleteMessages,
                        }, function( e, d ) { console.log( 'deleteBatch' ); console.log( e ); console.log( d ) } );
                        context.done( messages );
                    });
            });
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