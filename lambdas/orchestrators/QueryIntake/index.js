const { v4: uuidv4 } = require('uuid');
const AWS = require('aws-sdk');

/**
 * Query Intake Lambda Function
 * 
 * This Lambda function serves as the entry point for the student query system.
 * It receives requests from the React frontend, validates them,
 * generates correlation IDs, and forwards the queries to the LLM Query Analyzer.
 * 
 * Flow:
 * 1. Receive request from React frontend
 * 2. Validate request structure and authenticate/authorize if needed
 * 3. Generate correlation ID for request tracking
 * 4. Forward the request to LLM Query Analyzer Lambda
 * 5. Return the response to the frontend
 */

// AWS service clients
const lambda = new AWS.Lambda();

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
        const userId = extractUserId(event);
        
        if (!userMessage) {
            return formatErrorResponse(400, 'Missing required field: message');
        }
        
        // Generate a unique correlation ID for tracking this request flow
        const correlationId = uuidv4();
        console.log(`Generated correlation ID: ${correlationId}`);
        
        // Prepare payload for LLM Query Analyzer
        const analyzerPayload = {
            correlationId,
            userId,
            message: userMessage,
            timestamp: new Date().toISOString()
        };
        
        // Pass the request to the LLM Query Analyzer Lambda
        const analyzerResponse = await lambda.invoke({
            FunctionName: 'student-query-llm-analyzer',
            InvocationType: 'RequestResponse', // Synchronous invocation
            Payload: JSON.stringify(analyzerPayload)
        }).promise();
        
        // Parse and return the analyzer response
        const responsePayload = JSON.parse(analyzerResponse.Payload);
        
        return {
            statusCode: responsePayload.statusCode || 200,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*', // For CORS
                'Access-Control-Allow-Credentials': true
            },
            body: JSON.stringify({
                correlationId,
                message: responsePayload.body ? JSON.parse(responsePayload.body).message : 'Processing your request',
                status: responsePayload.statusCode === 200 ? 'success' : 'error'
            })
        };
        
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
    // API Gateway event
    if (event.body) {
        const body = typeof event.body === 'string' ? JSON.parse(event.body) : event.body;
        return body.message;
    }
    
    // Direct Lambda invocation
    return event.message;
}

/**
 * Extract the user ID from the incoming event
 * 
 * @param {Object} event - The Lambda event
 * @returns {string} - The extracted user ID or anonymous
 */
function extractUserId(event) {
    // From API Gateway with Cognito authorizer
    if (event.requestContext && event.requestContext.authorizer) {
        return event.requestContext.authorizer.claims['sub'] || 'anonymous';
    }
    
    // From direct Lambda invocation
    return event.userId || 'anonymous';
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
            'Access-Control-Allow-Credentials': true
        },
        body: JSON.stringify({
            message,
            status: 'error'
        })
    };
}
