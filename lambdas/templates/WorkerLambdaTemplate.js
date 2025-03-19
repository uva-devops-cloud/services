const AWS = require('aws-sdk');

/**
 * Worker Lambda Template
 * 
 * This template demonstrates how to structure a worker Lambda
 * to properly interact with the orchestrator architecture and
 * format responses for WebSocket delivery.
 * 
 * Flow:
 * 1. Receive event from Worker Dispatcher (via EventBridge)
 * 2. Extract query details and parameters
 * 3. Execute data retrieval logic specific to this worker
 * 4. Format response with proper metadata
 * 5. Publish response to EventBridge for ResponseAggregator
 */

// AWS service clients
const eventBridge = new AWS.EventBridge();
const documentClient = new AWS.DynamoDB.DocumentClient();

// Configuration
const CONFIG = {
    eventBusName: process.env.EVENT_BUS_NAME || 'main-event-bus',
    requestsTableName: process.env.REQUESTS_TABLE_NAME || 'StudentQueryRequests'
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - Event from EventBridge 
 * @param {Object} context - Lambda context
 * @returns {Object} - Success/failure response
 */
exports.handler = async (event, context) => {
    console.log('Received event:', JSON.stringify(event));
    
    try {
        // Extract data from the event
        const { 
            correlationId, 
            userId, 
            message,
            parameters // Any specific parameters for this worker
        } = event.detail;
        
        if (!correlationId) {
            throw new Error('Missing correlationId in event');
        }
        
        // Execute worker-specific data retrieval logic
        const data = await retrieveData(userId, parameters);
        
        // Format and publish the response
        await publishResponse({
            correlationId,
            userId,
            message,
            source: 'StudentDetails', // Replace with actual source name
            data
        });
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Data processed successfully',
                correlationId
            })
        };
        
    } catch (error) {
        console.error('Error in worker lambda:', error);
        
        // If we have a correlationId, publish an error response
        if (event.detail && event.detail.correlationId) {
            await publishResponse({
                correlationId: event.detail.correlationId,
                userId: event.detail.userId,
                message: event.detail.message,
                source: 'StudentDetails', // Replace with actual source name
                error: error.message,
                status: 'error'
            });
        }
        
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: `Error processing data: ${error.message}`,
                correlationId: event.detail?.correlationId
            })
        };
    }
};

/**
 * Retrieve data specific to this worker
 * 
 * @param {string} userId - User ID
 * @param {Object} parameters - Query parameters
 * @returns {Object} - Retrieved data
 */
async function retrieveData(userId, parameters) {
    // This is where you implement the specific data retrieval logic
    // for this worker lambda, such as:
    // - Query RDS database
    // - Fetch from DynamoDB
    // - Call external APIs
    // - Process and transform the data
    
    // Example implementation:
    /*
    const params = {
        TableName: 'Students',
        Key: { UserId: userId }
    };
    
    const result = await documentClient.get(params).promise();
    return result.Item;
    */
    
    // For now, return mock data
    return {
        studentId: '12345',
        name: 'Jane Smith',
        email: userId + '@university.edu',
        program: 'Computer Science',
        yearOfStudy: 3,
        enrollmentStatus: 'Active',
        credits: 85,
        gpa: 3.7
    };
}

/**
 * Publish response to EventBridge
 * 
 * @param {Object} responseData - Data to publish
 * @returns {Promise} - EventBridge putEvents promise
 */
async function publishResponse(responseData) {
    const { 
        correlationId, 
        userId, 
        source,
        data,
        error,
        status = 'success'
    } = responseData;
    
    const detail = {
        correlationId,
        userId,
        timestamp: new Date().toISOString(),
        source,
        status,
        data: data || null
    };
    
    // Add error information if present
    if (error) {
        detail.error = error;
        detail.status = 'error';
    }
    
    const params = {
        Entries: [
            {
                Source: 'student.query.worker',
                DetailType: `${source}Response`,
                Detail: JSON.stringify(detail),
                EventBusName: CONFIG.eventBusName
            }
        ]
    };
    
    console.log('Publishing event:', JSON.stringify(params));
    
    // Update DynamoDB to mark this data source as processed
    await updateRequestStatus(correlationId, source, status);
    
    return eventBridge.putEvents(params).promise();
}

/**
 * Update request status in DynamoDB
 * 
 * @param {string} correlationId - Correlation ID
 * @param {string} source - Data source name
 * @param {string} status - Processing status
 * @returns {Promise} - DynamoDB update promise
 */
async function updateRequestStatus(correlationId, source, status) {
    const timestamp = new Date().toISOString();
    
    const params = {
        TableName: CONFIG.requestsTableName,
        Key: { CorrelationId: correlationId },
        UpdateExpression: 'SET ProcessedSources.#source = :status, UpdatedAt = :timestamp',
        ExpressionAttributeNames: {
            '#source': source
        },
        ExpressionAttributeValues: {
            ':status': status,
            ':timestamp': timestamp
        }
    };
    
    return documentClient.update(params).promise();
}
