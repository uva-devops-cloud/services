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
    responsesTableName: process.env.RESPONSES_TABLE_NAME || 'StudentQueryResponses',
    conversationTableName: process.env.CONVERSATION_TABLE_NAME || 'ConversationMemory'
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
        
        // Store conversation history
        await storeConversationHistory(userId, correlationId, message, finalAnswer);
        
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
    
    // Get conversation history
    const userId = data.userId;
    const conversationHistory = await getConversationHistory(userId);
    
    // System prompt to instruct LLM to generate a final answer
    const systemPrompt = `You are a helpful academic advisor for university students.
    Use the following data to answer the student's question clearly and concisely.
    Make sure to reference specific information from the provided data to support your answer.
    Do not invent any information not included in the data.
    
    Available data: ${JSON.stringify(data, null, 2)}`;
    
    try {
        // Prepare messages array with conversation history
        const messages = [
            { role: 'system', content: systemPrompt }
        ];
        
        // Add conversation history if available
        if (conversationHistory && conversationHistory.length > 0) {
            // Add a note about conversation history to help the LLM understand context
            messages.push({ 
                role: 'system', 
                content: 'The following is recent conversation history with this student:' 
            });
            
            // Add each conversation turn
            conversationHistory.forEach(conv => {
                messages.push({ role: 'user', content: conv.question });
                messages.push({ role: 'assistant', content: conv.answer });
            });
            
            // Add a separator to distinguish history from current query
            messages.push({ 
                role: 'system', 
                content: 'Now answer this new question based on the conversation context above and the provided data:' 
            });
        }
        
        // Add the current query
        messages.push({ role: 'user', content: originalMessage });
        
        // Call the LLM API
        const response = await axios.post(
            CONFIG.llmEndpoint,
            {
                model: 'gpt-4',
                messages: messages,
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
 * Store conversation history in DynamoDB
 * 
 * @param {string} userId - User ID
 * @param {string} correlationId - Correlation ID for the request
 * @param {string} question - Student's question
 * @param {string} answer - Generated answer
 * @returns {Promise} - Promise resolving to DynamoDB put operation
 */
async function storeConversationHistory(userId, correlationId, question, answer) {
    console.log('Storing conversation history for user:', userId);
    
    // Set expiration time (15 minutes from now)
    const expirationTime = Math.floor(Date.now() / 1000) + (15 * 60);
    
    const params = {
        TableName: CONFIG.conversationTableName,
        Item: {
            UserId: userId,
            CorrelationId: correlationId,
            Timestamp: Date.now(),
            Question: question,
            Answer: answer,
            ExpirationTime: expirationTime
        }
    };
    
    return dynamoDB.put(params).promise();
}

/**
 * Get conversation history for a user
 * 
 * @param {string} userId - User ID
 * @returns {Promise<Array>} - Promise resolving to array of conversation history items
 */
async function getConversationHistory(userId) {
    console.log('Retrieving conversation history for user:', userId);
    
    const params = {
        TableName: CONFIG.conversationTableName,
        IndexName: 'UserConversationsIndex',
        KeyConditionExpression: 'UserId = :userId',
        ExpressionAttributeValues: {
            ':userId': userId
        },
        ScanIndexForward: false // Get most recent conversations first
    };
    
    try {
        const result = await dynamoDB.query(params).promise();
        return result.Items || [];
    } catch (error) {
        console.error('Error retrieving conversation history:', error);
        return [];
    }
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
