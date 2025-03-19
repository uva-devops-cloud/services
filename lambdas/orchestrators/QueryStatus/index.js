const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const jwt = require('jsonwebtoken');

// Configuration from environment variables
const CONFIG = {
  requestsTableName: process.env.REQUESTS_TABLE_NAME,
  responsesTableName: process.env.RESPONSES_TABLE_NAME
};

/**
 * Lambda handler for the QueryStatus function
 * Retrieves the status of a query based on its correlation ID
 */
exports.handler = async (event) => {
  try {
    console.log('Event received:', JSON.stringify(event));
    
    // Extract the correlation ID from the path parameters
    const correlationId = event.pathParameters?.correlationId;
    if (!correlationId) {
      return formatResponse(400, { error: 'Missing correlation ID' });
    }
    
    // Try to get user ID from JWT token in auth header (manual validation)
    const authHeader = event.headers ? (event.headers.Authorization || event.headers.authorization) : null;
    console.log('Authorization header:', authHeader ? `${authHeader.substring(0, 20)}...` : 'Not provided');
    
    let userId;
    
    // First try the standard authorizer way
    if (event.requestContext?.authorizer?.claims?.sub) {
      userId = event.requestContext.authorizer.claims.sub;
      console.log('Got userId from authorizer claims:', userId);
    } 
    // If that fails, try manual token validation
    else {
      const tokenValidation = validateToken(authHeader);
      if (!tokenValidation.valid) {
        console.error('Token validation failed:', tokenValidation.message);
        return formatResponse(401, { error: 'Unauthorized: Invalid token' });
      }
      userId = tokenValidation.userId;
      console.log('Got userId from manual token validation:', userId);
    }
    
    if (!userId) {
      return formatResponse(401, { error: 'Unauthorized - User ID not found' });
    }
    
    // Get the request status from DynamoDB
    const requestStatus = await getRequestStatus(correlationId, userId);
    if (!requestStatus) {
      return formatResponse(404, { error: 'Query not found or not authorized' });
    }
    
    // Get the response data if available
    let responseData = null;
    if (requestStatus.Status === 'COMPLETED') {
      responseData = await getQueryResponse(correlationId);
    }
    
    // Format the response
    const response = {
      correlationId: correlationId,
      status: requestStatus.Status,
      createdAt: requestStatus.CreatedAt,
      updatedAt: requestStatus.UpdatedAt,
      query: requestStatus.Query,
      response: responseData
    };
    
    return formatResponse(200, response);
  } catch (error) {
    console.error('Error processing request:', error);
    return formatResponse(500, { error: 'Internal server error' });
  }
};

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
 * Get the status of a request from DynamoDB
 */
async function getRequestStatus(correlationId, userId) {
  const params = {
    TableName: CONFIG.requestsTableName,
    Key: { CorrelationId: correlationId }
  };
  
  const result = await dynamoDB.get(params).promise();
  const item = result.Item;
  
  // Verify the user has access to this query
  if (!item || item.UserId !== userId) {
    return null;
  }
  
  return item;
}

/**
 * Get the response data for a completed query
 */
async function getQueryResponse(correlationId) {
  const params = {
    TableName: CONFIG.responsesTableName,
    KeyConditionExpression: 'CorrelationId = :cid',
    ExpressionAttributeValues: {
      ':cid': correlationId
    },
    ScanIndexForward: false, // Get the most recent response first
    Limit: 1
  };
  
  const result = await dynamoDB.query(params).promise();
  if (result.Items && result.Items.length > 0) {
    return result.Items[0].Answer;  // Changed from Response to Answer
  }
  
  return null;
}

/**
 * Format the API Gateway response
 */
function formatResponse(statusCode, body) {
  return {
    statusCode: statusCode,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true
    },
    body: JSON.stringify(body)
  };
}
