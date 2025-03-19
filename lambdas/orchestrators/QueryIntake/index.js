const AWS = require('aws-sdk');
const jwt = require('jsonwebtoken');

/**
 * Query Intake Lambda Function
 * 
 * This Lambda function serves as the entry point for retrieving stored answers.
 * It validates requests and tokens, then retrieves answers from DynamoDB.
 */

// AWS service clients
const dynamoDB = new AWS.DynamoDB.DocumentClient();

// Environment variables
const RESPONSES_TABLE = process.env.RESPONSES_TABLE_NAME || 'StudentQueryResponses';

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - API Gateway event
 * @param {Object} context - Lambda context
 * @returns {Object} - Response containing the stored answer or error
 */
exports.handler = async (event, context) => {
    try {
        console.log('Received request:', JSON.stringify(event));
        
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
        
        // Extract correlation ID from request
        const correlationId = extractCorrelationId(event);
        
        if (!correlationId) {
            return formatErrorResponse(400, 'No correlation ID provided in the request');
        }
        
        // Validate the token from the Authorization header
        const authHeader = event.headers ? (event.headers.Authorization || event.headers.authorization) : null;
        const tokenValidation = validateToken(authHeader);
        
        if (!tokenValidation.valid) {
            console.error('Token validation failed:', tokenValidation.message);
            return formatErrorResponse(401, 'Unauthorized: Invalid token');
        }
        
        // Extract user ID from token
        const userId = tokenValidation.userId;
        
        // Retrieve answer from DynamoDB
        const answer = await retrieveAnswer(correlationId, userId);
        
        if (!answer) {
            return formatErrorResponse(404, 'Answer not found');
        }
        
        // Return the answer
        return formatSuccessResponse(200, {
            correlationId,
            message: answer.Answer,
            status: 'success'
        });
        
    } catch (error) {
        console.error('Error in Query Intake Lambda:', error);
        return formatErrorResponse(500, `An error occurred: ${error.message}`);
    }
};

/**
 * Extract the correlation ID from the incoming event
 * 
 * @param {Object} event - The Lambda event
 * @returns {string} - The correlation ID
 */
function extractCorrelationId(event) {
    try {
        // If the event has a body property (API Gateway), parse it
        if (event.body) {
            const body = typeof event.body === 'string' ? JSON.parse(event.body) : event.body;
            return body.correlationId;
        }
        
        // If the event is already the payload (direct Lambda invocation)
        if (event.correlationId) {
            return event.correlationId;
        }
        
        return null;
    } catch (error) {
        console.error('Error extracting correlation ID:', error);
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
            
            // For development, accept any well-formed token
            return { 
                valid: true, 
                token,
                userId: decodedToken.payload.sub,
                username: decodedToken.payload.username || decodedToken.payload.sub
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
 * Retrieve answer from DynamoDB
 * 
 * @param {string} correlationId - The correlation ID
 * @param {string} userId - The user ID
 * @returns {Promise<Object>} - The stored answer or null if not found
 */
async function retrieveAnswer(correlationId, userId) {
    const params = {
        TableName: RESPONSES_TABLE,
        Key: {
            CorrelationId: correlationId,
            UserId: userId
        }
    };
    
    const result = await dynamoDB.get(params).promise();
    return result.Item || null;
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
