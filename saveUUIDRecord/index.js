'use strict';
console.log('Loading function');

let uuid = require('uuid');
let doc = require('dynamodb-doc');
let dynamo = new doc.DynamoDB();

/**
 * Guarantees a uuid is included as part of the insertion
 *
 *   - tableName: required for operations that interact with DynamoDB
 *   - uuidPropertyName: property in the table that contains the UUID
 *   - recordValues: list of AttributeValues to insert
 *   - list of any 
 */
exports.handler = function( event, context ) {
    console.log('Received event:', JSON.stringify(event, null, 2));

    var newUUID = uuid.v4();
    var item = event.recordValues;
    item[event.uuidPropertyName] = newUUID;
    
    var conditionExpression = event.uuidPropertyName + ' <> :0';
    var expressionAttributeValues = { ":0" : { "S" : newUUID } };

    var success = { UserID : newUUID, Username : item.Username }; 
    
    var duplicateUUID = dynamo.putItem( {
        TableName : event.tableName,
        Item : item,
        ConditionExpression : conditionExpression,
        ExpressionAttributeValues : expressionAttributeValues
    }, function( err, data ) {
        if( err ) {
            if( err == doc.ConditionalCheckFailedException ) {
                exports.insertUUIDRecord( event, context );
            } else {
                context.done(err);
            }
        } else {
	console.log( JSON.stringify( success, null, 2 ) );
            context.succeed( success );
        }
    } );

};
