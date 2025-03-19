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
 * Structured logging helper
 */
function log(level, message, data = {}) {
    const timestamp = new Date().toISOString();
    const logEntry = {
        timestamp,
        level,
        component: "ResponseAggregator",
        message,
        ...data
    };
    console.log(JSON.stringify(logEntry));
}

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - EventBridge event with worker lambda response
 * @param {Object} context - Lambda context
 * @returns {Object} - Success/failure response
 */
exports.handler = async (event, context) => {
    const startTime = Date.now();
    log('INFO', '🚀 Response Aggregator invoked', {
        requestId: context.awsRequestId,
        remainingTime: context.getRemainingTimeInMillis(),
        eventSource: event.source,
        eventDetailType: event['detail-type']
    });
    
    log('DEBUG', '📦 Received event', {
        event: JSON.stringify(event)
    });
    
    try {
        // Verify this is a worker response event
        if (!isWorkerResponseEvent(event)) {
            log('INFO', '🚫 Not a worker response event, ignoring', {
                source: event.source,
                detailType: event['detail-type']
            });
            return { statusCode: 200, message: 'Not a worker response event' };
        }
        
        // Extract response data from the event
        log('INFO', '🔍 Extracting response data from event');
        const { correlationId, workerName, data, timestamp } = extractResponseData(event);
        
        log('INFO', '✅ Extracted response data', { 
            correlationId,
            workerName,
            timestamp,
            hasData: !!data
        });
        
        if (!correlationId || !workerName || !data) {
            log('ERROR', '❌ Missing required parameters in worker response', {
                hasCorrelationId: !!correlationId,
                hasWorkerName: !!workerName,
                hasData: !!data
            });
            throw new Error('Missing required parameters in worker response');
        }
        
        // Store the worker response
        log('INFO', '💾 Storing worker response in DynamoDB', { 
            correlationId,
            workerName,
            tableName: CONFIG.requestsTableName 
        });
        
        const storeStartTime = Date.now();
        await storeWorkerResponse(correlationId, workerName, data, timestamp);
        
        log('INFO', '✅ Worker response stored successfully', {
            correlationId,
            workerName,
            duration: `${Date.now() - storeStartTime}ms`
        });
        
        // Check if all expected responses have been received
        log('INFO', '🔄 Checking if request is complete', { correlationId });
        const checkStartTime = Date.now();
        const isComplete = await checkRequestCompletion(correlationId);
        
        log('INFO', `${isComplete ? '✅ Request complete' : '⏳ Request incomplete'}`, {
            correlationId,
            isComplete,
            duration: `${Date.now() - checkStartTime}ms`
        });
        
        if (isComplete) {
            // Retrieve all response data for this request
            log('INFO', '📊 Retrieving all responses for completed request', { correlationId });
            const retrieveStartTime = Date.now();
            const aggregatedData = await retrieveAllResponses(correlationId);
            
            log('INFO', '✅ Retrieved all responses successfully', {
                correlationId,
                responseCount: Object.keys(aggregatedData.responses).length,
                duration: `${Date.now() - retrieveStartTime}ms`
            });
            
            // Forward to Answer Generator lambda
            log('INFO', '🚀 Forwarding to Answer Generator', { 
                correlationId, 
                answerGeneratorFunction: CONFIG.answerGeneratorFunction 
            });
            
            const forwardStartTime = Date.now();
            await forwardToAnswerGenerator(correlationId, aggregatedData);
            
            log('INFO', '✅ Successfully forwarded to Answer Generator', {
                correlationId,
                duration: `${Date.now() - forwardStartTime}ms`
            });
            
            const totalDuration = Date.now() - startTime;
            log('INFO', '🏁 Request processing complete', {
                correlationId,
                totalDuration: `${totalDuration}ms`
            });
            
            return { 
                statusCode: 200, 
                message: 'Request complete, forwarded to Answer Generator',
                correlationId,
                processingTime: totalDuration
            };
        }
        
        const totalDuration = Date.now() - startTime;
        log('INFO', '⏳ Request still awaiting more responses', {
            correlationId,
            totalDuration: `${totalDuration}ms`
        });
        
        return { 
            statusCode: 200, 
            message: 'Response stored, waiting for additional responses',
            correlationId,
            processingTime: totalDuration
        };
        
    } catch (error) {
        const totalDuration = Date.now() - startTime;
        log('ERROR', '❌ Error in Response Aggregator Lambda', {
            errorMessage: error.message,
            errorName: error.name,
            errorStack: error.stack,
            totalDuration: `${totalDuration}ms`
        });
        
        return {
            statusCode: 500,
            message: `An error occurred: ${error.message}`,
            processingTime: totalDuration
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
    log('DEBUG', '🔍 Checking if event is a worker response', {
        source: event.source,
        detailType: event['detail-type']
    });
    
    const isWorkerEvent = event.source === 'student.query.worker';
    const hasDetailType = !!event['detail-type'];
    const isResponseType = hasDetailType && event['detail-type'].endsWith('Response');
    
    const result = isWorkerEvent && hasDetailType && isResponseType;
    
    log('DEBUG', result ? '✅ Event is a worker response' : '❌ Event is not a worker response', {
        isWorkerEvent,
        hasDetailType,
        isResponseType,
        result
    });
    
    return result;
}

/**
 * Extract response data from the event
 * 
 * @param {Object} event - The EventBridge event
 * @returns {Object} - Extracted correlation ID, worker name, data, and timestamp
 */
function extractResponseData(event) {
    log('DEBUG', '🔄 Extracting data from event', {
        detailType: event['detail-type'],
        detailFormat: typeof event.detail
    });
    
    // Handle both cases: when detail is a string or already an object
    let detail;
    
    if (typeof event.detail === 'string') {
        log('DEBUG', '🔄 Event detail is a string, attempting to parse');
        try {
            detail = JSON.parse(event.detail || '{}');
            log('DEBUG', '✅ Successfully parsed detail string to object');
        } catch (e) {
            log('ERROR', '❌ Error parsing event.detail string', {
                error: e.message,
                detail: event.detail ? event.detail.substring(0, 100) + '...' : 'empty'
            });
            detail = {};
        }
    } else {
        log('DEBUG', '✅ Event detail is already an object');
        detail = event.detail || {};
    }
    
    // Get the worker name by removing 'Response' suffix
    const workerName = event['detail-type'].replace('Response', '');
    
    log('DEBUG', '🔍 Extracted detail object', {
        workerName,
        hasCorrelationId: !!detail.correlationId,
        hasData: !!detail.data
    });
    
    log('DEBUG', '📦 Detail contents', {
        detail: JSON.stringify(detail)
    });
    
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
    log('DEBUG', '💾 Preparing DynamoDB update operation', {
        correlationId,
        workerName,
        tableName: CONFIG.requestsTableName,
        timestamp
    });
    
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
    
    log('DEBUG', '🔄 Executing DynamoDB update operation', { 
        correlationId,
        expression: params.UpdateExpression
    });
    
    try {
        const result = await dynamoDB.update(params).promise();
        log('DEBUG', '✅ DynamoDB update successful', {
            correlationId,
            updatedAttributes: Object.keys(result.Attributes || {}).join(', ')
        });
        return result;
    } catch (error) {
        log('ERROR', '❌ DynamoDB update failed', {
            correlationId,
            errorMessage: error.message,
            errorCode: error.code,
            tableName: CONFIG.requestsTableName
        });
        throw error;
    }
}

/**
 * Check if all expected responses for a request have been received
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @returns {boolean} - Whether all expected responses have been received
 */
async function checkRequestCompletion(correlationId) {
    log('DEBUG', '🔍 Checking request completion status', { correlationId });
    
    const params = {
        TableName: CONFIG.requestsTableName,
        Key: { CorrelationId: correlationId }
    };
    
    try {
        log('DEBUG', '🔄 Fetching request data from DynamoDB', { correlationId });
        const result = await dynamoDB.get(params).promise();
        
        if (!result.Item) {
            log('ERROR', '❌ Request not found in DynamoDB', { correlationId });
            throw new Error(`Request with correlation ID ${correlationId} not found`);
        }
        
        const request = result.Item;
        const requiredSources = request.RequiredSources || [];
        const receivedResponses = request.ReceivedResponses || [];
        const receivedWorkers = new Set(receivedResponses.map(r => r.WorkerName));
        
        log('DEBUG', '🔢 Comparing required sources vs received responses', {
            correlationId,
            requiredSources: requiredSources.join(', '),
            receivedWorkers: Array.from(receivedWorkers).join(', '),
            requiredCount: requiredSources.length,
            receivedCount: receivedWorkers.size
        });
        
        // Check if all required sources have responded
        const isComplete = requiredSources.every(source => receivedWorkers.has(source));
        
        log('DEBUG', isComplete ? '✅ All required responses received' : '⏳ Still waiting for some responses', {
            correlationId,
            isComplete
        });
        
        if (isComplete && !request.IsComplete) {
            // Update the request status to complete
            log('INFO', '🔄 Updating request status to complete', { correlationId });
            
            try {
                await dynamoDB.update({
                    TableName: CONFIG.requestsTableName,
                    Key: { CorrelationId: correlationId },
                    UpdateExpression: 'SET IsComplete = :complete, Status = :status',
                    ExpressionAttributeValues: {
                        ':complete': true,
                        ':status': 'COMPLETE'
                    }
                }).promise();
                
                log('INFO', '✅ Request status updated to complete', { correlationId });
            } catch (updateError) {
                log('ERROR', '❌ Failed to update request status', {
                    correlationId,
                    errorMessage: updateError.message
                });
                // Don't throw error here, we still want to proceed with the completion logic
            }
        }
        
        return isComplete;
    } catch (error) {
        log('ERROR', '❌ Error checking request completion', {
            correlationId,
            errorMessage: error.message,
            errorCode: error.code
        });
        throw error;
    }
}

/**
 * Retrieve all responses for a request
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @returns {Object} - Original request and all responses
 */
async function retrieveAllResponses(correlationId) {
    log('DEBUG', '🔍 Retrieving all responses for request', { correlationId });
    
    const params = {
        TableName: CONFIG.requestsTableName,
        Key: { CorrelationId: correlationId }
    };
    
    try {
        const result = await dynamoDB.get(params).promise();
        
        if (!result.Item) {
            log('ERROR', '❌ Request not found in DynamoDB during response retrieval', { correlationId });
            throw new Error(`Request with correlation ID ${correlationId} not found`);
        }
        
        log('DEBUG', '✅ Retrieved request data from DynamoDB', {
            correlationId,
            userId: result.Item.UserId,
            responseCount: (result.Item.ReceivedResponses || []).length
        });
        
        // Transform responses into a more usable format
        log('DEBUG', '🔄 Transforming response data to aggregated format', { correlationId });
        const transformedResponses = {};
        result.Item.ReceivedResponses.forEach(response => {
            transformedResponses[response.WorkerName] = response.Data;
            log('DEBUG', `📊 Mapped response for ${response.WorkerName}`, {
                correlationId,
                workerName: response.WorkerName,
                timestamp: response.Timestamp,
                dataSize: JSON.stringify(response.Data).length
            });
        });
        
        const aggregatedData = {
            correlationId,
            userId: result.Item.UserId,
            message: result.Item.Message,
            responses: transformedResponses
        };
        
        log('DEBUG', '✅ Response data aggregation complete', {
            correlationId,
            responseCount: Object.keys(transformedResponses).length
        });
        
        return aggregatedData;
    } catch (error) {
        log('ERROR', '❌ Error retrieving all responses', {
            correlationId,
            errorMessage: error.message,
            errorCode: error.code
        });
        throw error;
    }
}

/**
 * Forward aggregated data to the Answer Generator lambda
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @param {Object} aggregatedData - Aggregated response data
 * @returns {Promise} - Promise resolving to Lambda invoke response
 */
async function forwardToAnswerGenerator(correlationId, aggregatedData) {
    log('DEBUG', '🚀 Preparing to invoke Answer Generator', {
        correlationId,
        functionName: CONFIG.answerGeneratorFunction
    });
    
    const params = {
        FunctionName: CONFIG.answerGeneratorFunction,
        InvocationType: 'Event', // Asynchronous invocation
        Payload: JSON.stringify(aggregatedData)
    };
    
    log('DEBUG', '📦 Payload prepared for Answer Generator', {
        correlationId,
        payloadSize: JSON.stringify(aggregatedData).length,
        responseCount: Object.keys(aggregatedData.responses || {}).length
    });
    
    try {
        const result = await lambda.invoke(params).promise();
        
        log('INFO', '✅ Answer Generator Lambda invoked successfully', {
            correlationId,
            statusCode: result.StatusCode,
            requestId: result.ResponseMetadata ? result.ResponseMetadata.RequestId : 'unknown'
        });
        
        return result;
    } catch (error) {
        log('ERROR', '❌ Error invoking Answer Generator Lambda', {
            correlationId,
            errorMessage: error.message,
            errorCode: error.code,
            functionName: CONFIG.answerGeneratorFunction
        });
        throw error;
    }
}
