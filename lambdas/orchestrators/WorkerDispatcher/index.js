const AWS = require('aws-sdk');
const { Client } = require('pg');

/**
 * Worker Dispatcher Lambda Function
 * 
 * This Lambda function is responsible for dispatching events to the appropriate
 * worker Lambdas based on the analysis from the LLM Query Analyzer.
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

// Logging helper
function logWithTime(level, message, data = {}) {
    const timestamp = new Date().toISOString();
    const logData = {
        timestamp,
        level,
        message,
        ...data
    };
    console.log(JSON.stringify(logData));
}

exports.handler = async (event, context) => {
    const startTime = Date.now();
    logWithTime('INFO', '🚀 Worker Dispatcher invoked', { 
        requestId: context.awsRequestId,
        memoryLimit: context.memoryLimitInMB,
        remainingTime: context.getRemainingTimeInMillis()
    });
    
    // Log environment configuration
    logWithTime('DEBUG', '📋 Environment configuration', {
        eventBusName: CONFIG.eventBusName,
        requestsTableName: CONFIG.requestsTableName,
        dbHost: CONFIG.dbHost,
        dbName: CONFIG.dbName,
        dbSecretArnExists: !!CONFIG.dbSecretArn
    });
    
    try {
        logWithTime('INFO', '📬 Received event payload', {
            hasCorrelationId: !!event.correlationId,
            hasUserId: !!event.userId,
            requiredDataCount: event.requiredData ? event.requiredData.length : 0,
            eventType: typeof event
        });
        
        // Extract the required information from the event
        const { correlationId, userId, originalMessage, requiredData, llmAnalysis } = event;
        
        if (!correlationId) {
            logWithTime('ERROR', '❌ Missing correlation ID in request');
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: 'Missing correlation ID',
                    status: 'error'
                })
            };
        }
        
        if (!userId) {
            logWithTime('ERROR', '❌ Missing user ID in request', { correlationId });
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: 'Missing user ID',
                    status: 'error'
                })
            };
        }
        
        if (!requiredData || requiredData.length === 0) {
            logWithTime('ERROR', '❌ No required data sources specified', { correlationId, userId });
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: 'No required data sources specified',
                    status: 'error'
                })
            };
        }

        // Look up student ID from database
        logWithTime('INFO', '🔍 Looking up student ID', { correlationId, userId });
        const dbStartTime = Date.now();
        
        let studentId;
        try {
            studentId = await getStudentIdFromCognito(userId);
            const dbDuration = Date.now() - dbStartTime;
            logWithTime('INFO', '✅ Database lookup completed', { 
                correlationId, 
                duration: `${dbDuration}ms`,
                found: !!studentId 
            });
        } catch (dbError) {
            logWithTime('ERROR', '❌ Database lookup failed', {
                correlationId,
                errorType: dbError.constructor.name,
                errorMessage: dbError.message,
                errorStack: dbError.stack,
                duration: `${Date.now() - dbStartTime}ms`
            });
            
            throw dbError; // Re-throw to be caught by main try/catch
        }
        
        if (!studentId) {
            logWithTime('ERROR', '❌ Student ID not found for user', { correlationId, userId });
            return {
                statusCode: 404,
                body: JSON.stringify({
                    message: 'Student ID not found for user',
                    status: 'error'
                })
            };
        }
        
        // Track the request in DynamoDB
        logWithTime('INFO', '📝 Storing request metadata in DynamoDB', { 
            correlationId, 
            tableName: CONFIG.requestsTableName 
        });
        
        // const dynamoStartTime = Date.now();
        // try {
        //     await storeRequestMetadata(correlationId, userId, studentId, originalMessage, requiredData);
        //     const dynamoDuration = Date.now() - dynamoStartTime;
        //     logWithTime('INFO', '✅ Request metadata stored successfully', { 
        //         correlationId, 
        //         duration: `${dynamoDuration}ms` 
        //     });
        // } catch (dynamoError) {
        //     logWithTime('ERROR', '❌ Failed to store request metadata', {
        //         correlationId,
        //         errorType: dynamoError.constructor.name,
        //         errorMessage: dynamoError.message,
        //         errorStack: dynamoError.stack,
        //         duration: `${Date.now() - dynamoStartTime}ms`
        //     });
            
        //     throw dynamoError; // Re-throw to be caught by main try/catch
        // }
        
        // Dispatch events to worker lambdas
        logWithTime('INFO', '📡 Dispatching events to workers', { 
            correlationId, 
            workerCount: requiredData.length,
            sources: requiredData.map(d => d.source).join(', ')
        });
        
        const dispatchStartTime = Date.now();
        let dispatchedEvents;
        try {
            dispatchedEvents = await dispatchEventsToWorkers(correlationId, userId, studentId, requiredData);
            const dispatchDuration = Date.now() - dispatchStartTime;
            
            const successCount = dispatchedEvents.filter(e => e.status === 'dispatched').length;
            logWithTime('INFO', '✅ Events dispatched to workers', { 
                correlationId, 
                successCount: successCount,
                totalCount: dispatchedEvents.length,
                duration: `${dispatchDuration}ms`
            });
        } catch (dispatchError) {
            logWithTime('ERROR', '❌ Error dispatching events', {
                correlationId,
                errorType: dispatchError.constructor.name,
                errorMessage: dispatchError.message,
                errorStack: dispatchError.stack,
                duration: `${Date.now() - dispatchStartTime}ms`
            });
            
            throw dispatchError; // Re-throw to be caught by main try/catch
        }
        
        const totalDuration = Date.now() - startTime;
        logWithTime('INFO', '🏁 Worker Dispatcher completed successfully', { 
            correlationId, 
            totalDuration: `${totalDuration}ms`,
            workersTriggered: dispatchedEvents.length
        });
        
        return {
            statusCode: 200,
            workersTriggered: dispatchedEvents.length,
            workersDetails: dispatchedEvents
        };
        
    } catch (error) {
        const totalDuration = Date.now() - startTime;
        logWithTime('ERROR', '💥 Unhandled exception in Worker Dispatcher', { 
            errorType: error.constructor.name,
            errorMessage: error.message,
            errorStack: error.stack,
            totalDuration: `${totalDuration}ms`
        });
        
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
 */
async function getStudentIdFromCognito(cognitoUserId) {
    logWithTime('DEBUG', '🔄 Starting student ID lookup', { cognitoUserId: cognitoUserId.substring(0, 8) + '...' });
    
    let client = null;
    try {
        logWithTime('DEBUG', '🔌 Connecting to database');
        client = await connectToDatabase();
        logWithTime('DEBUG', '✅ Database connection established');
        
        const query = 'SELECT student_id FROM students WHERE cognito_info = $1';
        logWithTime('DEBUG', '🔍 Executing database query', { query });
        
        const queryStartTime = Date.now();
        const result = await client.query(query, [cognitoUserId]);
        const queryDuration = Date.now() - queryStartTime;
        
        logWithTime('DEBUG', '📋 Query results received', { 
            rowCount: result.rowCount,
            duration: `${queryDuration}ms`
        });
        
        if (result.rows.length > 0) {
            const studentId = result.rows[0].student_id;
            logWithTime('DEBUG', '✅ Student ID found', { 
                studentId,
                cognitoUserId: cognitoUserId.substring(0, 8) + '...'
            });
            return studentId;
        }
        
        logWithTime('WARN', '⚠️ No student record found for this Cognito user', { 
            cognitoUserId: cognitoUserId.substring(0, 8) + '...'
        });
        return null;
    } catch (error) {
        logWithTime('ERROR', '❌ Error during database query', {
            errorType: error.constructor.name,
            errorCode: error.code,
            errorMessage: error.message,
            detail: error.detail || 'No additional details'
        });
        throw error;
    } finally {
        if (client) {
            try {
                logWithTime('DEBUG', '🔌 Closing database connection');
                await client.end();
                logWithTime('DEBUG', '✅ Database connection closed');
            } catch (closeError) {
                logWithTime('WARN', '⚠️ Error closing database connection', { 
                    errorMessage: closeError.message 
                });
            }
        }
    }
}

/**
 * Connect to the database
 */
async function connectToDatabase() {
    logWithTime('DEBUG', '🔑 Retrieving database credentials');
    
    try {
        // Check if we have the necessary configuration
        if (!CONFIG.dbSecretArn) {
            throw new Error('Missing DB_SECRET_ARN environment variable');
        }
        if (!CONFIG.dbHost) {
            throw new Error('Missing DB_HOST environment variable');
        }
        if (!CONFIG.dbName) {
            throw new Error('Missing DB_NAME environment variable');
        }
        
        const secretStartTime = Date.now();
        const secretResponse = await secretsManager.getSecretValue({
            SecretId: CONFIG.dbSecretArn
        }).promise();
        const secretDuration = Date.now() - secretStartTime;
        
        logWithTime('DEBUG', '✅ Database credentials retrieved', { 
            duration: `${secretDuration}ms`
        });
        
        const password = secretResponse.SecretString;
        if (!password) {
            throw new Error('Empty password retrieved from Secrets Manager');
        }
        
        // Log connection parameters without the password
        logWithTime('DEBUG', '⚙️ Creating database client with parameters', {
            host: CONFIG.dbHost,
            database: CONFIG.dbName,
            user: 'dbadmin',
            port: CONFIG.dbPort || 5432
        });
        
        const client = new Client({
            host: CONFIG.dbHost,
            database: CONFIG.dbName,
            user: 'dbadmin',
            password: password,
            port: CONFIG.dbPort || 5432,
            // Add connection timeout for better error diagnostics
            connectionTimeoutMillis: 10000
        });
        
        logWithTime('DEBUG', '🔄 Initiating database connection');
        const connectStartTime = Date.now();
        await client.connect();
        const connectDuration = Date.now() - connectStartTime;
        
        logWithTime('DEBUG', '✅ Database connection established successfully', { 
            duration: `${connectDuration}ms` 
        });
        
        return client;
    } catch (error) {
        logWithTime('ERROR', '❌ Database connection failed', {
            errorType: error.constructor.name,
            errorMessage: error.message,
            errorCode: error.code,
            errorStack: error.stack,
            dbHost: CONFIG.dbHost,
            dbName: CONFIG.dbName,
            hasDbSecretArn: !!CONFIG.dbSecretArn
        });
        throw new Error(`Database connection error: ${error.message}`);
    }
}

/**
 * Store request metadata in DynamoDB for tracking
 */
async function storeRequestMetadata(correlationId, userId, studentId, message, requiredData) {
    const timestamp = new Date().toISOString();
    const sources = requiredData.map(item => item.source);
    
    logWithTime('DEBUG', '📝 Preparing DynamoDB item', { 
        correlationId, 
        sources: sources.join(', ')
    });
    
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
    
    try {
        logWithTime('DEBUG', '💾 Writing to DynamoDB', { 
            tableName: CONFIG.requestsTableName,
            correlationId
        });
        
        const putStartTime = Date.now();
        const result = await dynamoDB.put({
            TableName: CONFIG.requestsTableName,
            Item: item
        }).promise();
        const putDuration = Date.now() - putStartTime;
        
        logWithTime('DEBUG', '✅ DynamoDB write successful', { 
            correlationId,
            duration: `${putDuration}ms` 
        });
        
        return result;
    } catch (error) {
        logWithTime('ERROR', '❌ DynamoDB write failed', {
            correlationId,
            tableName: CONFIG.requestsTableName,
            errorType: error.constructor.name,
            errorMessage: error.message,
            errorCode: error.code
        });
        throw error;
    }
}

/**
 * Dispatch events to worker lambdas
 */
async function dispatchEventsToWorkers(correlationId, userId, studentId, requiredData) {
    const dispatchedEvents = [];
    logWithTime('DEBUG', '📡 Beginning event dispatch process', { 
        correlationId, 
        sourceCount: requiredData.length 
    });
    
    for (const dataSource of requiredData) {
        const eventDetail = {
            correlationId,
            userId,
            studentId,
            action: dataSource.source,
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
        
        logWithTime('DEBUG', '🔄 Dispatching event', { 
            correlationId, 
            source: dataSource.source,
            eventBusName: CONFIG.eventBusName,
            hasParams: Object.keys(dataSource.params || {}).length > 0
        });
        
        try {
            const dispatchStartTime = Date.now();
            const result = await eventBridge.putEvents(params).promise();
            const dispatchDuration = Date.now() - dispatchStartTime;
            
            const eventId = result.Entries && result.Entries[0] ? result.Entries[0].EventId : 'unknown';
            const failedCount = result.FailedEntryCount || 0;
            
            if (failedCount > 0) {
                logWithTime('WARN', '⚠️ Event dispatch partially failed', { 
                    correlationId,
                    source: dataSource.source,
                    failedCount,
                    duration: `${dispatchDuration}ms`
                });
            } else {
                logWithTime('DEBUG', '✅ Event dispatched successfully', { 
                    correlationId,
                    source: dataSource.source,
                    eventId,
                    duration: `${dispatchDuration}ms`
                });
            }
            
            dispatchedEvents.push({
                source: dataSource.source,
                eventId: eventId,
                status: 'dispatched'
            });
        } catch (error) {
            logWithTime('ERROR', '❌ Event dispatch failed', {
                correlationId,
                source: dataSource.source,
                errorType: error.constructor.name,
                errorMessage: error.message,
                errorCode: error.code,
                eventBusName: CONFIG.eventBusName
            });
            
            dispatchedEvents.push({
                source: dataSource.source,
                status: 'error',
                error: error.message
            });
        }
    }
    
    logWithTime('INFO', '📊 Event dispatch summary', { 
        correlationId,
        totalEvents: requiredData.length,
        successCount: dispatchedEvents.filter(e => e.status === 'dispatched').length,
        failureCount: dispatchedEvents.filter(e => e.status === 'error').length
    });
    
    return dispatchedEvents;
}
