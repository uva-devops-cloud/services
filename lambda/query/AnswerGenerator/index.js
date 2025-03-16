const AWS = require('aws-sdk');
const axios = require('axios');

// AWS service clients
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const secretsManager = new AWS.SecretsManager();

// Configuration
const CONFIG = {
    llmEndpoint: process.env.LLM_ENDPOINT || 'your-llm-endpoint',
    llmApiKeySecretArn: process.env.LLM_API_KEY_SECRET_ARN,
    requestsTableName: process.env.REQUESTS_TABLE_NAME || 'StudentQueryRequests',
    responsesTableName: process.env.RESPONSES_TABLE_NAME || 'StudentQueryResponses'
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - Event from Response Aggregator Lambda
 * @param {Object} context - Lambda context
 * @returns {Object} - Success/failure response
 */
exports.handler = async (event, context) => {
    try {
        console.log('Received aggregated data:', JSON.stringify(event));
        
        // Extract data from the event
        const { correlationId, userId, message, responses } = event;
        
        if (!correlationId || !message || !responses) {
            return { 
                statusCode: 400, 
                body: JSON.stringify({
                    message: 'Missing required data in event',
                    status: 'error'
                })
            };
        }
        
        // Format the data for the LLM
        const formattedData = formatDataForLLM(message, responses);
        
        // Call the LLM for final answer generation
        const finalAnswer = await callLLMForAnswer(message, formattedData);
        
        // Store the final answer in DynamoDB
        await storeResponse(correlationId, userId, message, finalAnswer);
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                correlationId,
                message: finalAnswer,
                status: 'complete'
            })
        };
        
    } catch (error) {
        console.error('Error in Answer Generator Lambda:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: `An error occurred in the Answer Generator: ${error.message}`,
                status: 'error'
            })
        };
    }
};

/**
 * Format aggregated data for the LLM
 * 
 * @param {string} originalMessage - Original student question
 * @param {Array} responses - Responses from worker lambdas
 * @returns {Object} - Formatted data for LLM
 */
function formatDataForLLM(originalMessage, responses) {
    const formattedData = {};
    
    // Group responses by source
    for (const response of responses) {
        formattedData[response.Source] = response.Data;
    }
    
    return formattedData;
}

/**
 * Call the LLM for final answer generation
 * 
 * @param {string} originalMessage - Original student question
 * @param {Object} data - Formatted data from worker lambdas
 * @returns {string} - Final answer from LLM
 */
async function callLLMForAnswer(originalMessage, data) {
    console.log('Calling LLM for final answer with data:', JSON.stringify(data));
    
    // Get LLM API key from Secrets Manager
    const apiKey = await getApiKeyFromSecrets();
    
    // System prompt to instruct LLM to generate a final answer
    const systemPrompt = `You are a helpful academic advisor for university students.
    Use the following data to answer the student's question clearly and concisely.
    Make sure to reference specific information from the provided data to support your answer.
    Do not invent any information not included in the data.
    
    Available data: ${JSON.stringify(data, null, 2)}`;
    
    try {
        // This is a placeholder. Replace with actual LLM API call.
        const response = await axios.post(
            CONFIG.llmEndpoint,
            {
                model: 'gpt-4',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: originalMessage }
                ],
                temperature: 0.7
            },
            {
                headers: {
                    'Authorization': `Bearer ${apiKey}`,
                    'Content-Type': 'application/json'
                }
            }
        );
        
        return response.data.choices[0].message.content;
    } catch (error) {
        console.error('Error calling LLM API for final answer:', error);
        
        // Fallback response if LLM call fails
        return `I've analyzed your academic data regarding: "${originalMessage}". However, I encountered an issue generating a detailed response. Please try again or contact academic services for assistance.`;
    }
}

/**
 * Store the final response in DynamoDB
 * 
 * @param {string} correlationId - Correlation ID for the request
 * @param {string} userId - User ID
 * @param {string} question - Original question
 * @param {string} answer - Final answer from LLM
 * @returns {Promise} - Promise resolving to DynamoDB put operation
 */
async function storeResponse(correlationId, userId, question, answer) {
    const timestamp = new Date().toISOString();
    
    const item = {
        CorrelationId: correlationId,
        UserId: userId,
        Timestamp: timestamp,
        Question: question,
        Answer: answer,
        TTL: Math.floor(Date.now() / 1000) + 2592000 // 30 days TTL
    };
    
    return dynamoDB.put({
        TableName: CONFIG.responsesTableName,
        Item: item
    }).promise();
}

/**
 * Get the LLM API key from AWS Secrets Manager
 * 
 * @returns {string} - The API key
 */
async function getApiKeyFromSecrets() {
    try {
        const data = await secretsManager.getSecretValue({
            SecretId: CONFIG.llmApiKeySecretArn
        }).promise();
        
        const secret = JSON.parse(data.SecretString);
        return secret.api_key;
    } catch (error) {
        console.error('Error retrieving API key from Secrets Manager:', error);
        throw new Error('Failed to retrieve LLM API key');
    }
}
