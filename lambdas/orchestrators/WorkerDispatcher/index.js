const AWS = require('aws-sdk');
const { Client } = require('pg');

/**
 * Worker Dispatcher Lambda Function
 * 
 * This Lambda function is responsible for dispatching events to the appropriate
 * worker Lambdas based on the analysis from the LLM Query Analyzer.
 * 
 * Flow:
 * 1. Receive analyzed query with required data sources from LLM Query Analyzer
 * 2. Look up student ID from database using Cognito user ID
 * 3. Create and publish appropriate EventBridge events for worker lambdas
 * 4. Store request tracking information in DynamoDB
 * 5. Return confirmation of dispatched events
 */

// AWS service clients
const eventBridge = new AWS.EventBridge();
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const secretsManager = new AWS.SecretsManager();

// Configuration
const CONFIG = {
    eventBusName: process.env.EVENT_BUS_NAME || 'main-event-bus',
    requestsTableName: process.env.REQUESTS_TABLE_NAME || 'StudentQueryRequests',
    dbSecretArn: process.env.DB_SECRET_ARN,
    dbHost: process.env.DB_HOST,
    dbName: process.env.DB_NAME,
    dbPort: process.env.DB_PORT
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - Event from LLM Query Analyzer Lambda
 * @param {Object} context - Lambda context
 * @returns {Object} - Response with dispatched events information
 */
exports.handler = async (event, context) => {
    try {
        console.log('Received request from LLM Query Analyzer:', JSON.stringify(event));
        
        // Extract the required information from the event
        const { correlationId, userId, originalMessage, requiredData, llmAnalysis } = event;
        
        if (!requiredData || requiredData.length === 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: 'No required data sources specified',
                    status: 'error'
                })
            };
        }

        // Look up student ID from database
        const studentId = await getStudentIdFromCognito(userId);
        if (!studentId) {
            return {
                statusCode: 404,
                body: JSON.stringify({
                    message: 'Student ID not found for user',
                    status: 'error'
                })
            };
        }
        
        // Track the request in DynamoDB
        await storeRequestMetadata(correlationId, userId, studentId, originalMessage, requiredData);
        
        // Dispatch events to worker lambdas
        const dispatchedEvents = await dispatchEventsToWorkers(correlationId, userId, studentId, requiredData);
        
        return {
            statusCode: 200,
            workersTriggered: dispatchedEvents.length,
            workersDetails: dispatchedEvents
        };
        
    } catch (error) {
        console.error('Error in Worker Dispatcher Lambda:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: `An error occurred in the Worker Dispatcher: ${error.message}`,
                status: 'error'
            })
        };
    }
};

/**
 * Get student ID from database using Cognito user ID
 * 
 * @param {string} cognitoUserId - Cognito user ID
 * @returns {Promise<string>} - Student ID
 */
async function getStudentIdFromCognito(cognitoUserId) {
    const client = await connectToDatabase();
    
    try {
        const result = await client.query(
            'SELECT student_id FROM students WHERE cognito_info = $1',
            [cognitoUserId]
        );
        
        if (result.rows.length > 0) {
            return result.rows[0].student_id;
        }
        
        return null;
    } finally {
        await client.end();
    }
}

/**
 * Connect to the database
 * 
 * @returns {Promise<Client>} - Database client
 */
async function connectToDatabase() {
    try {
        const secretResponse = await secretsManager.getSecretValue({
            SecretId: CONFIG.dbSecretArn
        }).promise();
        
        const password = secretResponse.SecretString;
        
        const client = new Client({
            host: CONFIG.dbHost,
            database: CONFIG.dbName,
            user: 'dbadmin',
            password: password,
            port: CONFIG.dbPort
        });
        
        await client.connect();
        return client;
    } catch (error) {
        console.error('Error connecting to database:', error);
        throw new Error(`Database connection error: ${error.message}`);
    }
}

/**
 * Store request metadata in DynamoDB for tracking
 * 
 * @param {string} correlationId - Unique ID for tracking the request
 * @param {string} userId - ID of the user making the request
 * @param {string} studentId - Student ID from database
 * @param {string} message - Original message from the user
 * @param {Array} requiredData - Data sources required for the response
 * @returns {Promise} - Promise resolving to DynamoDB PutItem response
 */
async function storeRequestMetadata(correlationId, userId, studentId, message, requiredData) {
    const timestamp = new Date().toISOString();
    const sources = requiredData.map(item => item.source);
    
    const item = {
        CorrelationId: correlationId,
        UserId: userId,
        StudentId: studentId,
        Message: message,
        RequiredSources: sources,
        Status: 'PENDING',
        CreatedAt: timestamp,
        TTL: Math.floor(Date.now() / 1000) + 86400, // 24 hours TTL
        ReceivedResponses: [],
        IsComplete: false
    };
    
    return dynamoDB.put({
        TableName: CONFIG.requestsTableName,
        Item: item
    }).promise();
}

/**
 * Dispatch events to worker lambdas
 * 
 * @param {string} correlationId - Unique ID for tracking the request
 * @param {string} userId - ID of the user making the request
 * @param {string} studentId - Student ID from database
 * @param {Array} requiredData - Data sources required for the response
 * @returns {Array} - Array of dispatched event details
 */
async function dispatchEventsToWorkers(correlationId, userId, studentId, requiredData) {
    const dispatchedEvents = [];
    
    for (const dataSource of requiredData) {
        const eventDetail = {
            correlationId,
            userId,
            studentId,
            params: dataSource.params || {}
        };
        
        const params = {
            Entries: [
                {
                    Source: 'student.query.orchestrator',
                    DetailType: dataSource.source,
                    Detail: JSON.stringify(eventDetail),
                    EventBusName: CONFIG.eventBusName
                }
            ]
        };
        
        try {
            const result = await eventBridge.putEvents(params).promise();
            console.log(`Event dispatched for ${dataSource.source}:`, result);
            
            dispatchedEvents.push({
                source: dataSource.source,
                eventId: result.Entries[0].EventId,
                status: 'dispatched'
            });
        } catch (error) {
            console.error(`Error dispatching event for ${dataSource.source}:`, error);
            
            dispatchedEvents.push({
                source: dataSource.source,
                status: 'error',
                error: error.message
            });
        }
    }
    
    return dispatchedEvents;
}
