const { v4: uuidv4 } = require('uuid');
const AWS = require('aws-sdk');
const jwt = require('jsonwebtoken');

/**
 * Query Intake Lambda Function
 * 
 * This Lambda function serves as the entry point for the student query system.
 * It receives requests from the React frontend, validates them,
 * generates correlation IDs, and forwards the queries to the LLM Query Analyzer.
 * 
 * Flow:
 * 1. Receive request from React frontend
 * 2. Validate request structure and authenticate/authorize using JWT
 * 3. Generate correlation ID for request tracking
 * 4. Store initial request in DynamoDB
 * 5. Forward the request to LLM Query Analyzer Lambda
 * 6. Return the response to the frontend
 */

// AWS service clients
const lambda = new AWS.Lambda();
const dynamoDB = new AWS.DynamoDB.DocumentClient();

// Environment variables
const REQUESTS_TABLE = process.env.REQUESTS_TABLE_NAME || 'StudentQueryRequests';
const CONVERSATION_TABLE = process.env.CONVERSATION_TABLE_NAME || 'ConversationMemory';
const LLM_QUERY_ANALYZER_FUNCTION = process.env.LLM_QUERY_ANALYZER_FUNCTION || 'LLMQueryAnalyzer';

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - API Gateway event
 * @param {Object} context - Lambda context
 * @returns {Object} - Response to be returned to the frontend or next lambda in chain
 */
exports.handler = async (event, context) => {
    try {
        console.log('Received request:', JSON.stringify(event));
        
        // Extract user message from the event
        const userMessage = extractUserMessage(event);
        
        // Validate and extract user identity from JWT
        const userIdentity = await validateAndExtractUserIdentity(event);
        
        if (!userIdentity.isValid) {
            return formatErrorResponse(401, 'Unauthorized: Invalid or missing authentication');
        }
        
        if (!userMessage) {
            return formatErrorResponse(400, 'Missing required field: message');
        }
        
        // Generate a unique correlation ID for tracking this request flow
        const correlationId = uuidv4();
        console.log(`Generated correlation ID: ${correlationId}`);
        
        // Store the initial request in DynamoDB
        await storeInitialRequest(correlationId, userIdentity.userId, userMessage);
        
        // Prepare payload for LLM Query Analyzer
        const analyzerPayload = {
            correlationId,
            userId: userIdentity.userId,
            userEmail: userIdentity.email,
            userName: userIdentity.name,
            message: userMessage,
            timestamp: new Date().toISOString()
        };
        
        // Invoke LLM Query Analyzer Lambda
        const analyzerResponse = await lambda.invoke({
            FunctionName: LLM_QUERY_ANALYZER_FUNCTION,
            InvocationType: 'RequestResponse',
            Payload: JSON.stringify(analyzerPayload)
        }).promise();
        
        // Parse the response from the LLM Query Analyzer
        const analyzerResult = JSON.parse(analyzerResponse.Payload);
        console.log('Analyzer response:', JSON.stringify(analyzerResult));
        
        // Check if we have a direct response (no worker lambdas needed)
        if (analyzerResult.directResponse) {
            // Store the direct response in DynamoDB
            await storeDirectResponse(correlationId, userIdentity.userId, userMessage, analyzerResult.directResponse);
            
            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Origin'
                },
                body: JSON.stringify({
                    correlationId,
                    message: 'Query processed successfully',
                    status: 'complete',
                    answer: analyzerResult.directResponse
                })
            };
        }
        
        // Return the correlation ID to the frontend for status polling
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Origin'
            },
            body: JSON.stringify({
                correlationId,
                message: 'Query received and processing',
                status: 'processing'
            })
        };
        
    } catch (error) {
        console.error('Error processing request:', error);
        return formatErrorResponse(500, `Error processing request: ${error.message}`);
    }
};

/**
 * Extract the user message from the incoming event
 * 
 * @param {Object} event - The Lambda event
 * @returns {string} - The extracted user message
 */
function extractUserMessage(event) {
    try {
        // If the event has a body property (API Gateway), parse it
        if (event.body) {
            const body = typeof event.body === 'string' ? JSON.parse(event.body) : event.body;
            return body.message;
        }
        
        // If the event is already the payload (direct Lambda invocation)
        if (event.message) {
            return event.message;
        }
        
        return null;
    } catch (error) {
        console.error('Error extracting user message:', error);
        return null;
    }
}

/**
 * Validate and extract user identity from JWT token
 * 
 * @param {Object} event - The Lambda event
 * @returns {Object} - Object containing userId, email, name, and isValid flag
 */
async function validateAndExtractUserIdentity(event) {
    try {
        // Default response for invalid authentication
        const defaultResponse = {
            userId: 'anonymous',
            email: '',
            name: '',
            isValid: false
        };
        
        // Extract the Authorization header
        let token = null;
        
        if (event.headers && event.headers.Authorization) {
            token = event.headers.Authorization.replace('Bearer ', '');
        } else if (event.headers && event.headers.authorization) {
            token = event.headers.authorization.replace('Bearer ', '');
        }
        
        // For direct Lambda invocation, the token might be in the payload
        if (!token && event.token) {
            token = event.token;
        }
        
        if (!token) {
            console.warn('No authentication token provided');
            return defaultResponse;
        }
        
        // Decode the JWT token (without verification for now)
        // In production, you should verify the token signature
        const decoded = jwt.decode(token);
        
        if (!decoded || !decoded.sub) {
            console.warn('Invalid token format');
            return defaultResponse;
        }
        
        return {
            userId: decoded.sub,
            email: decoded.email || '',
            name: decoded.name || `${decoded.given_name || ''} ${decoded.family_name || ''}`.trim(),
            isValid: true
        };
        
    } catch (error) {
        console.error('Error validating user identity:', error);
        return {
            userId: 'anonymous',
            email: '',
            name: '',
            isValid: false
        };
    }
}

/**
 * Store the initial request in DynamoDB
 * 
 * @param {string} correlationId - The correlation ID
 * @param {string} userId - The user ID
 * @param {string} message - The user message
 * @returns {Promise} - Promise resolving to the DynamoDB response
 */
async function storeInitialRequest(correlationId, userId, message) {
    const timestamp = new Date().toISOString();
    const ttl = Math.floor(Date.now() / 1000) + (30 * 24 * 60 * 60); // 30 days TTL
    
    const params = {
        TableName: REQUESTS_TABLE,
        Item: {
            CorrelationId: correlationId,
            Timestamp: timestamp,
            UserId: userId,
            Message: message,
            Status: 'processing',
            TTL: ttl
        }
    };
    
    return dynamoDB.put(params).promise();
}

/**
 * Store a direct response in DynamoDB (when no worker lambdas are needed)
 * 
 * @param {string} correlationId - The correlation ID
 * @param {string} userId - The user ID
 * @param {string} question - The original question
 * @param {string} answer - The direct answer
 * @returns {Promise} - Promise resolving to the DynamoDB response
 */
async function storeDirectResponse(correlationId, userId, question, answer) {
    const timestamp = new Date().toISOString();
    const ttl = Math.floor(Date.now() / 1000) + (30 * 24 * 60 * 60); // 30 days TTL
    
    const params = {
        TableName: process.env.RESPONSES_TABLE_NAME || 'StudentQueryResponses',
        Item: {
            CorrelationId: correlationId,
            Timestamp: timestamp,
            UserId: userId,
            Question: question,
            Answer: answer,
            TTL: ttl
        }
    };
    
    return dynamoDB.put(params).promise();
}

/**
 * Format an error response
 * 
 * @param {number} statusCode - HTTP status code
 * @param {string} message - Error message
 * @returns {Object} - Formatted error response
 */
function formatErrorResponse(statusCode, message) {
    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Origin'
        },
        body: JSON.stringify({
            message,
            status: 'error'
        })
    };
}
