const AWS = require('aws-sdk');

/**
 * WebSocket Connect Handler Lambda
 * 
 * This Lambda handles WebSocket connections for real-time updates
 * between the orchestrator system and the frontend.
 * 
 * Flow:
 * 1. Client connects to WebSocket API
 * 2. This Lambda stores the connection ID with user info in DynamoDB
 * 3. Future responses can be pushed to the client using this connection
 */

// AWS service clients
const dynamoDB = new AWS.DynamoDB.DocumentClient();

// Configuration
const CONFIG = {
    connectionsTableName: process.env.CONNECTIONS_TABLE_NAME || 'WebSocketConnections',
    defaultTTL: 3600 // 1 hour in seconds
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
        console.log('WebSocket $connect event:', JSON.stringify(event));
        
        // Extract connection ID from event
        const connectionId = event.requestContext.connectionId;
        if (!connectionId) {
            return { statusCode: 400, body: 'Missing connection ID' };
        }
        
        // Extract user info from query parameters or auth
        const userId = getUserIdFromEvent(event);
        const correlationId = getQueryParameter(event, 'correlationId');
        
        // Store connection details in DynamoDB
        await storeConnection(connectionId, userId, correlationId);
        
        return { statusCode: 200, body: 'Connected' };
        
    } catch (error) {
        console.error('Error in WebSocket Connect Lambda:', error);
        return { statusCode: 500, body: `Connection failed: ${error.message}` };
    }
};

/**
 * Extract user ID from event (query params or auth)
 * 
 * @param {Object} event - API Gateway event
 * @returns {string} - User ID or anonymous
 */
function getUserIdFromEvent(event) {
    // First try from query string
    const userIdFromQuery = getQueryParameter(event, 'userId');
    if (userIdFromQuery) return userIdFromQuery;
    
    // Then check Cognito authorizer if present
    if (event.requestContext && 
        event.requestContext.authorizer && 
        event.requestContext.authorizer.claims &&
        event.requestContext.authorizer.claims.sub) {
        return event.requestContext.authorizer.claims.sub;
    }
    
    // Default to anonymous with a timestamp
    return `anonymous-${Date.now()}`;
}

/**
 * Get a query parameter from the WebSocket connect event
 * 
 * @param {Object} event - API Gateway event
 * @param {string} paramName - Parameter name
 * @returns {string|null} - Parameter value or null
 */
function getQueryParameter(event, paramName) {
    if (event.queryStringParameters && event.queryStringParameters[paramName]) {
        return event.queryStringParameters[paramName];
    }
    return null;
}

/**
 * Store connection details in DynamoDB
 * 
 * @param {string} connectionId - WebSocket connection ID
 * @param {string} userId - User ID
 * @param {string} correlationId - Optional correlation ID for specific query
 * @returns {Promise} - DynamoDB put operation promise
 */
async function storeConnection(connectionId, userId, correlationId) {
    const timestamp = new Date().toISOString();
    const ttl = Math.floor(Date.now() / 1000) + CONFIG.defaultTTL;
    
    const item = {
        ConnectionId: connectionId,
        UserId: userId,
        ConnectedAt: timestamp,
        TTL: ttl
    };
    
    // Add correlation ID if present
    if (correlationId) {
        item.CorrelationId = correlationId;
    }
    
    return dynamoDB.put({
        TableName: CONFIG.connectionsTableName,
        Item: item
    }).promise();
}
