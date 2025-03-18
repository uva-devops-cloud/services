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
    console.log('Received event:', JSON.stringify(event));
    
    try {
        // Verify this is a worker response event
        if (!isWorkerResponseEvent(event)) {
            console.log('Not a worker response event, ignoring');
            return { statusCode: 200, message: 'Not a worker response event' };
        }
        
        // Extract response data from the event
        const { correlationId, workerName, data, timestamp } = extractResponseData(event);
        
        if (!correlationId || !workerName || !data) {
            throw new Error('Missing required parameters in worker response');
        }
        
        // Store the worker response
        await storeWorkerResponse(correlationId, workerName, data, timestamp);
        
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
    return event.source === 'student.query.worker' && 
           event['detail-type'] && 
           event['detail-type'].endsWith('Response');
}

/**
 * Extract response data from the event
 * 
 * @param {Object} event - The EventBridge event
 * @returns {Object} - Extracted correlation ID, worker name, data, and timestamp
 */
function extractResponseData(event) {
    const detail = JSON.parse(event.detail || '{}');
    const workerName = event['detail-type'].replace('Response', '');
    
    return {
        correlationId: detail.correlationId,
        workerName,
        data: detail.data,
        timestamp: detail.timestamp || new Date().toISOString()
    };
}

/**
 * Store worker response in DynamoDB
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @param {string} workerName - Name of the worker that responded
 * @param {Object} data - Response data from the worker
 * @param {string} timestamp - Timestamp of the response
 * @returns {Promise} - Promise resolving to DynamoDB update response
 */
async function storeWorkerResponse(correlationId, workerName, data, timestamp) {
    const params = {
        TableName: CONFIG.requestsTableName,
        Key: { CorrelationId: correlationId },
        UpdateExpression: 
            'SET ReceivedResponses = list_append(if_not_exists(ReceivedResponses, :empty_list), :response), ' +
            'UpdatedAt = :timestamp',
        ExpressionAttributeValues: {
            ':response': [{
                WorkerName: workerName,
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
    const receivedWorkers = new Set(receivedResponses.map(r => r.WorkerName));
    
    // Check if all required sources have responded
    const isComplete = requiredSources.every(source => receivedWorkers.has(source));
    
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
    
    // Transform responses into a more usable format
    const transformedResponses = {};
    result.Item.ReceivedResponses.forEach(response => {
        transformedResponses[response.WorkerName] = response.Data;
    });
    
    return {
        correlationId,
        userId: result.Item.UserId,
        message: result.Item.Message,
        responses: transformedResponses
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
