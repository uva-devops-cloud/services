const { CognitoIdentityServiceProvider } = require('aws-sdk');

/**
 * Lambda function to update Cognito user attributes for SSO users
 * This function has admin privileges to update attributes that normal
 * users (especially SSO users) don't have permission to update directly.
 */
exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event, null, 2));

    // CORS headers for browser requests
    const headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "OPTIONS,PUT",
        "Content-Type": "application/json"
    };

    // Handle preflight OPTIONS request
    if (event.httpMethod === 'OPTIONS') {
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ message: 'CORS preflight successful' })
        };
    }

    try {
        // Initialize the Cognito Identity Provider client
        const cognito = new CognitoIdentityServiceProvider();

        // Get USER_POOL_ID from environment variable (set in Terraform)
        const userPoolId = process.env.USER_POOL_ID;
        if (!userPoolId) {
            throw new Error('USER_POOL_ID environment variable is not set');
        }

        // Parse the request body
        const requestBody = JSON.parse(event.body || '{}');
        const { attributes } = requestBody;

        if (!Array.isArray(attributes) || attributes.length === 0) {
            return {
                statusCode: 400,
                headers,
                body: JSON.stringify({ error: 'Invalid or missing attributes in request body' })
            };
        }

        // Extract user information from the JWT token
        const auth = event.requestContext.authorizer;
        if (!auth || !auth.claims) {
            return {
                statusCode: 401,
                headers,
                body: JSON.stringify({ error: 'Authentication required' })
            };
        }

        // Get username from claims - either cognito:username or email or sub
        const username = auth.claims['cognito:username'] || auth.claims.email || auth.claims.sub;
        if (!username) {
            return {
                statusCode: 400,
                headers,
                body: JSON.stringify({ error: 'Could not determine username from token' })
            };
        }

        console.log(`Updating attributes for user: ${username}`);
        console.log('Attributes to update:', JSON.stringify(attributes, null, 2));

        // Call AdminUpdateUserAttributes API
        const result = await cognito.adminUpdateUserAttributes({
            UserPoolId: userPoolId,
            Username: username,
            UserAttributes: attributes
        }).promise();

        console.log('Update successful:', JSON.stringify(result, null, 2));

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                message: 'User attributes updated successfully',
                username
            })
        };
    } catch (error) {
        console.error('Error updating user attributes:', error);

        return {
            statusCode: error.statusCode || 500,
            headers,
            body: JSON.stringify({
                error: error.message || 'An error occurred updating user attributes'
            })
        };
    }
};