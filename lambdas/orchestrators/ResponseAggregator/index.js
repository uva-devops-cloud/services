const AWS = require('aws-sdk');

/**
 * Response Aggregator Lambda Function
 * 
 * This Lambda function collects and combines responses from worker lambdas,
 * tracking their completion status via correlation IDs.
 * 
 * Flow:
 * 1. Receive responses from worker lambdas through EventBridge
 * 2. Match responses to the original request using correlation ID
 * 3. Store responses in DynamoDB
 * 4. Check if all expected responses have been received
 * 5. If complete, forward aggregated data to Answer Generator lambda
 */

// AWS service clients
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const lambda = new AWS.Lambda();

// Configuration
const CONFIG = {
    requestsTableName: process.env.REQUESTS_TABLE_NAME || 'StudentQueryRequests',
    answerGeneratorFunction: process.env.ANSWER_GENERATOR_FUNCTION || 'student-query-answer-generator',
    conversationTableName: process.env.CONVERSATION_TABLE_NAME || 'ConversationMemory'
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - EventBridge event with worker lambda response
 * @param {Object} context - Lambda context
 * @returns {Object} - Success/failure response
 */
exports.handler = async (event, context) => {
    try {
        console.log('Received worker response event:', JSON.stringify(event));
        
        // Process only worker response events
        if (!isWorkerResponseEvent(event)) {
            return { statusCode: 200, message: 'Not a worker response event' };
        }
        
        // Extract correlation ID and source from the event
        const { correlationId, source, data } = extractResponseData(event);
        
        if (!correlationId || !source) {
            return { 
                statusCode: 400, 
                message: 'Missing required correlation ID or source in event' 
            };
        }
        
        // Store the response in DynamoDB
        await storeWorkerResponse(correlationId, source, data);
        
        // Check if all expected responses have been received
        const isComplete = await checkRequestCompletion(correlationId);
        
        if (isComplete) {
            // Retrieve all response data for this request
            const aggregatedData = await retrieveAllResponses(correlationId);
            
            // Forward to Answer Generator lambda
            await forwardToAnswerGenerator(correlationId, aggregatedData);
            
            return { 
                statusCode: 200, 
                message: 'Request complete, forwarded to Answer Generator',
                correlationId
            };
        }
        
        return { 
            statusCode: 200, 
            message: 'Response stored, waiting for additional responses',
            correlationId
        };
        
    } catch (error) {
        console.error('Error in Response Aggregator Lambda:', error);
        return {
            statusCode: 500,
            message: `An error occurred: ${error.message}`
        };
    }
};

/**
 * Check if the event is a worker response event
 * 
 * @param {Object} event - The EventBridge event
 * @returns {boolean} - Whether this is a worker response event
 */
function isWorkerResponseEvent(event) {
    // Check source pattern and detail type
    return event.source && 
           event.source.startsWith('student.query.worker') && 
           event['detail-type'] && 
           event['detail-type'].endsWith('Response');
}

/**
 * Extract response data from the event
 * 
 * @param {Object} event - The EventBridge event
 * @returns {Object} - Extracted correlation ID, source, and data
 */
function extractResponseData(event) {
    const detail = event.detail || {};
    const source = event['detail-type'].replace('Response', '');
    
    return {
        correlationId: detail.correlationId,
        source,
        data: detail.data || {}
    };
}

/**
 * Store worker response in DynamoDB
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @param {string} source - Source of the response (worker lambda type)
 * @param {Object} data - Response data from the worker
 * @returns {Promise} - Promise resolving to DynamoDB update response
 */
async function storeWorkerResponse(correlationId, source, data) {
    const timestamp = new Date().toISOString();
    
    const params = {
        TableName: CONFIG.requestsTableName,
        Key: { CorrelationId: correlationId },
        UpdateExpression: 
            'SET ReceivedResponses = list_append(if_not_exists(ReceivedResponses, :empty_list), :response), ' +
            'UpdatedAt = :timestamp',
        ExpressionAttributeValues: {
            ':response': [{
                Source: source,
                Timestamp: timestamp,
                Data: data
            }],
            ':empty_list': [],
            ':timestamp': timestamp
        },
        ReturnValues: 'UPDATED_NEW'
    };
    
    return dynamoDB.update(params).promise();
}

/**
 * Check if all expected responses for a request have been received
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @returns {boolean} - Whether all expected responses have been received
 */
async function checkRequestCompletion(correlationId) {
    const params = {
        TableName: CONFIG.requestsTableName,
        Key: { CorrelationId: correlationId }
    };
    
    const result = await dynamoDB.get(params).promise();
    
    if (!result.Item) {
        throw new Error(`Request with correlation ID ${correlationId} not found`);
    }
    
    const request = result.Item;
    const requiredSources = request.RequiredSources || [];
    const receivedResponses = request.ReceivedResponses || [];
    const receivedSources = new Set(receivedResponses.map(r => r.Source));
    
    // Check if all required sources have responded
    const isComplete = requiredSources.every(source => receivedSources.has(source));
    
    if (isComplete && !request.IsComplete) {
        // Update the request status to complete
        await dynamoDB.update({
            TableName: CONFIG.requestsTableName,
            Key: { CorrelationId: correlationId },
            UpdateExpression: 'SET IsComplete = :complete, Status = :status',
            ExpressionAttributeValues: {
                ':complete': true,
                ':status': 'COMPLETE'
            }
        }).promise();
    }
    
    return isComplete;
}

/**
 * Retrieve all responses for a request
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @returns {Object} - Original request and all responses
 */
async function retrieveAllResponses(correlationId) {
    const params = {
        TableName: CONFIG.requestsTableName,
        Key: { CorrelationId: correlationId }
    };
    
    const result = await dynamoDB.get(params).promise();
    
    if (!result.Item) {
        throw new Error(`Request with correlation ID ${correlationId} not found`);
    }
    
    return {
        correlationId,
        userId: result.Item.UserId,
        message: result.Item.Message,
        responses: result.Item.ReceivedResponses || []
    };
}

/**
 * Forward aggregated data to the Answer Generator lambda
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @param {Object} aggregatedData - Aggregated response data
 * @returns {Promise} - Promise resolving to Lambda invoke response
 */
async function forwardToAnswerGenerator(correlationId, aggregatedData) {
    const params = {
        FunctionName: CONFIG.answerGeneratorFunction,
        InvocationType: 'Event', // Asynchronous invocation
        Payload: JSON.stringify(aggregatedData)
    };
    
    return lambda.invoke(params).promise();
}
