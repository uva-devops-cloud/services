const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();

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
    
    // Extract user ID from the JWT claims
    const userId = event.requestContext.authorizer?.claims?.sub;
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
    return result.Items[0].Response;
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
