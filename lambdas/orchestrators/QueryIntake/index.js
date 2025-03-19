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
 * 4. Forward the request to LLM Query Analyzer Lambda
 * 5. Return the response to the frontend
 */

// AWS service clients
const lambda = new AWS.Lambda();
const dynamoDB = new AWS.DynamoDB.DocumentClient();

// Environment variables
const REQUESTS_TABLE = process.env.REQUESTS_TABLE_NAME || 'StudentQueryRequests';
const CONVERSATION_TABLE = process.env.CONVERSATION_TABLE_NAME || 'ConversationMemory';
const LLM_ANALYZER_FUNCTION = process.env.LLM_ANALYZER_FUNCTION || 'student-query-llm-analyzer';

const CONFIG = {
    llmAnalyzerFunction: process.env.LLM_ANALYZER_FUNCTION
};

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
        console.log('Authorization header:', event.headers ? event.headers.Authorization : 'No Authorization header');
        
        if (event.httpMethod === 'OPTIONS') {
            return {
                statusCode: 200,
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Origin'
                },
                body: JSON.stringify({})
            };
        }
        
        // Extract query from request
        const userMessage = extractUserMessage(event);
        
        if (!userMessage) {
            return formatErrorResponse(400, 'No message provided in the request body');
        }
        
        // Validate the token from the Authorization header
        const authHeader = event.headers ? (event.headers.Authorization || event.headers.authorization) : null;
        const tokenValidation = validateToken(authHeader);
        
        if (!tokenValidation.valid) {
            console.error('Token validation failed:', tokenValidation.message);
            return formatErrorResponse(401, 'Unauthorized: Invalid token');
        }
        
        // Generate a unique correlation ID for tracking
        const correlationId = uuidv4();
        
        // Extract user ID from token
        const userId = tokenValidation.userId;
        const username = tokenValidation.username;
        console.log(`Processing request for user ${userId} with message: ${userMessage}`);
        
        // Ensure user data exists by checking and generating if needed
        try {
            await ensureUserDataExists(userId, username);
        } catch (dataError) {
            console.error('Error ensuring user data exists:', dataError);
            // Continue with query - non-blocking error
        }
        
        // Store initial request in DynamoDB
        try {
            await storeInitialRequest(correlationId, userId, userMessage);
            console.log(`Stored initial request with correlation ID: ${correlationId}`);
        } catch (storageError) {
            console.error('Error storing initial request:', storageError);
            // Continue with query - non-blocking error
        }
        
        console.log("correlationId", correlationId);
        console.log("userId", userId);
        console.log("userMessage", userMessage);
        // Forward to LLM Query Analyzer Lambda
        const payload = {
            correlationId,
            userId,
            message: userMessage
        };
        
        console.log(`Invoking LLM Query Analyzer with payload: ${JSON.stringify(payload)}`);
        
        const lambdaResponse = await lambda.invoke({
            FunctionName: LLM_ANALYZER_FUNCTION,
            InvocationType: 'RequestResponse', // Synchronous invocation
            Payload: JSON.stringify(payload)
        }).promise();
        
        // Parse and process the Lambda response
        const responsePayload = JSON.parse(lambdaResponse.Payload);
        const responseBody = responsePayload.body ? JSON.parse(responsePayload.body) : {};
        
        console.log(`Received response from LLM Query Analyzer: ${JSON.stringify(responseBody)}`);
        
        // If the LLM determined no worker lambdas are needed, store the direct response
        if (responseBody.requiresWorkers === false) {
            try {
                await storeDirectResponse(correlationId, userId, userMessage, responseBody.message);
                console.log(`Stored direct response for correlation ID: ${correlationId}`);
            } catch (storageError) {
                console.error('Error storing direct response:', storageError);
                // Continue with response - non-blocking error
            }
        }
        
        // Return the response to the frontend
        return formatSuccessResponse(200, {
            correlationId,
            message: responseBody.message,
            requiresWorkers: responseBody.requiresWorkers,
            status: 'PENDING'
        });
        
    } catch (error) {
        console.error('Error in Query Intake Lambda:', error);
        return formatErrorResponse(500, `An error occurred: ${error.message}`);
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
 * Manually verify the token from the Authorization header
 * This is a fallback in case API Gateway authorizer fails
 * 
 * @param {string} authHeader - Authorization header
 * @returns {Object} - Token validation result
 */
function validateToken(authHeader) {
  try {
    console.log('Manual token validation started');
    
    if (!authHeader) {
      console.error('Authorization header is missing');
      return { valid: false, message: 'Authorization header is missing' };
    }

    // Extract the token from the Authorization header
    const tokenParts = authHeader.split(' ');
    if (tokenParts.length !== 2 || tokenParts[0] !== 'Bearer') {
      console.error('Authorization header format is invalid. Expected "Bearer token"');
      return { valid: false, message: 'Invalid Authorization header format' };
    }

    const token = tokenParts[1];
    console.log('Token extracted from Authorization header:', token.substring(0, 20) + '...');
    
    // For development purposes, simply validate token structure
    try {
      // Decode the JWT to extract user information (without verification for debugging)
      const decodedToken = jwt.decode(token, { complete: true });
      
      if (!decodedToken || !decodedToken.payload) {
        console.error('Invalid token format - could not decode');
        return { valid: false, message: 'Invalid token format' };
      }
      
      console.log('Token payload:', JSON.stringify(decodedToken.payload, null, 4));
      console.log('Token scopes:', decodedToken.payload.scope);
      
      // For development, accept any well-formed token
      return { 
        valid: true, 
        token,
        userId: decodedToken.payload.sub,
        username: decodedToken.payload.username || decodedToken.payload.sub,
        scopes: decodedToken.payload.scope || ''
      };
    } catch (decodeError) {
      console.error('Error decoding token:', decodeError);
      return { valid: false, message: `Error decoding token: ${decodeError.message}` };
    }
  } catch (error) {
    console.error('Error validating token:', error);
    return { valid: false, message: `Token validation error: ${error.message}` };
  }
}

/**
 * Ensure user data exists by checking and generating if needed
 * 
 * @param {string} userId - The user ID (Cognito sub)
 * @param {string} username - The username
 * @returns {Promise} - Promise resolving when user data is ensured
 */
async function ensureUserDataExists(userId, username) {
    console.log(`Checking if user data exists for user ${userId} (${username})`);
    
    try {
        // Call the UserDataGenerator Lambda
        const lambda = new AWS.Lambda();
        
        const params = {
            FunctionName: process.env.USER_DATA_GENERATOR_FUNCTION || 'student-query-user-data-generator',
            InvocationType: 'RequestResponse', // Synchronous to ensure data exists before continuing
            Payload: JSON.stringify({
                cognitoSub: userId,
                username: username
            })
        };
        
        console.log(`Invoking UserDataGenerator Lambda for user ${userId}`);
        const response = await lambda.invoke(params).promise();
        
        if (response.FunctionError) {
            throw new Error(`UserDataGenerator Lambda returned an error: ${response.Payload}`);
        }
        
        const payload = JSON.parse(response.Payload);
        const body = payload.body ? JSON.parse(payload.body) : {};
        
        console.log(`UserDataGenerator response: ${JSON.stringify(body)}`);
        
        return body;
    } catch (error) {
        console.error(`Error ensuring user data exists: ${error.message}`, error);
        throw error;
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
            Status: 'PROCESSING',
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
            Status: 'COMPLETED',
            TTL: ttl
        }
    };
    
    return dynamoDB.put(params).promise();
}

/**
 * Format a successful response with CORS headers
 * 
 * @param {number} statusCode - HTTP status code
 * @param {Object} body - Response body
 * @returns {Object} - Formatted successful response
 */
function formatSuccessResponse(statusCode, body) {
    return {
        statusCode: statusCode,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Origin'
        },
        body: JSON.stringify(body)
    };
}

/**
 * Format an error response with CORS headers
 * 
 * @param {number} statusCode - HTTP status code
 * @param {string} message - Error message
 * @returns {Object} - Formatted error response
 */
function formatErrorResponse(statusCode, message) {
    return {
        statusCode: statusCode,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Origin'
        },
        body: JSON.stringify({
            error: message
        })
    };
}
