const AWS = require('aws-sdk');

/**
 * WebSocket Disconnect Handler Lambda
 * 
 * This Lambda handles WebSocket disconnections and cleans up
 * connection records from DynamoDB.
 * 
 * Flow:
 * 1. Client disconnects from WebSocket API
 * 2. This Lambda removes the connection ID from DynamoDB
 */

// AWS service clients
const dynamoDB = new AWS.DynamoDB.DocumentClient();

// Configuration
const CONFIG = {
    connectionsTableName: process.env.CONNECTIONS_TABLE_NAME || 'WebSocketConnections'
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - API Gateway WebSocket event
 * @param {Object} context - Lambda context
 * @returns {Object} - Response
 */
exports.handler = async (event, context) => {
    try {
        console.log('WebSocket $disconnect event:', JSON.stringify(event));
        
        // Extract connection ID from event
        const connectionId = event.requestContext.connectionId;
        if (!connectionId) {
            return { statusCode: 400, body: 'Missing connection ID' };
        }
        
        // Delete connection from DynamoDB
        await deleteConnection(connectionId);
        
        return { statusCode: 200, body: 'Disconnected' };
        
    } catch (error) {
        console.error('Error in WebSocket Disconnect Lambda:', error);
        return { statusCode: 500, body: `Disconnect handling failed: ${error.message}` };
    }
};

/**
 * Delete connection details from DynamoDB
 * 
 * @param {string} connectionId - WebSocket connection ID
 * @returns {Promise} - DynamoDB delete operation promise
 */
async function deleteConnection(connectionId) {
    const params = {
        TableName: CONFIG.connectionsTableName,
        Key: { ConnectionId: connectionId }
    };
    
    return dynamoDB.delete(params).promise();
}
