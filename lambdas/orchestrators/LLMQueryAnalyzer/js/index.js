const AWS = require('aws-sdk');
const axios = require('axios');

/**
 * LLM Query Analyzer Lambda Function
 * 
 * This Lambda function sends student queries to the LLM for analysis and intent detection.
 * It determines which worker lambdas need to be invoked based on the question.
 * It also detects small talk and general questions, providing direct responses when appropriate.
 * 
 * Flow:
 * 1. Receive student query from Query Intake Lambda
 * 2. Retrieve conversation history from DynamoDB
 * 3. Send the query with conversation history to LLM for intent recognition
 * 4. Parse the LLM response to identify required data and worker lambdas
 * 5. If small talk is detected, respond directly
 * 6. Otherwise, forward structured analysis to Worker Dispatcher Lambda
 */

// AWS service clients
const lambda = new AWS.Lambda();
const secretsManager = new AWS.SecretsManager();
const dynamoDB = new AWS.DynamoDB.DocumentClient();

// Configuration
const CONFIG = {
    llmEndpoint: process.env.LLM_ENDPOINT || 'your-llm-endpoint',
    llmApiKeySecretArn: process.env.LLM_API_KEY_SECRET_ARN,
    workerDispatcherFunction: process.env.WORKER_DISPATCHER_FUNCTION || 'student-query-worker-dispatcher',
    conversationTableName: process.env.CONVERSATION_TABLE_NAME || 'ConversationMemory'
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - Event from Query Intake Lambda
 * @param {Object} context - Lambda context
 * @returns {Object} - Response with LLM analysis
 */
exports.handler = async (event, context) => {
    try {
        console.log('Received request from Query Intake:', JSON.stringify(event));
        
        // Extract the required information from the event
        const { correlationId, userId, message } = event;
        
        // Retrieve conversation history from DynamoDB
        const conversationHistory = await getConversationHistory(userId);
        
        // Check if this is small talk or a general question
        const smallTalkResponse = await checkForSmallTalk(message, conversationHistory);
        
        if (smallTalkResponse.isSmallTalk) {
            console.log('Small talk detected, providing direct response');
            
            // Store this conversation entry
            try {
                await storeConversationHistory(userId, correlationId, message, smallTalkResponse.response);
            } catch (storageError) {
                console.error('Error storing conversation history:', storageError);
                // Continue even if storage fails
            }
            
            return {
                statusCode: 200,
                body: JSON.stringify({
                    correlationId,
                    message: smallTalkResponse.response,
                    requiresWorkers: false,
                    isSmallTalk: true
                })
            };
        }
        
        // Not small talk, continue with regular flow
        // Call the LLM for query analysis with conversation history
        const llmResponse = await callLLMForAnalysis(message, conversationHistory);
        
        // Parse the LLM response to determine required data sources
        const requiredData = await parseRequiredDataSources(llmResponse);
        
        if (requiredData.length === 0) {
            // If no data sources needed, we can directly answer the question
            const directResponse = await callLLMForDirectAnswer(message, conversationHistory);
            
            // Store this conversation entry
            try {
                await storeConversationHistory(userId, correlationId, message, directResponse.content);
            } catch (storageError) {
                console.error('Error storing conversation history:', storageError);
                // Continue even if storage fails
            }
            
            return {
                statusCode: 200,
                body: JSON.stringify({
                    correlationId,
                    message: directResponse.content,
                    requiresWorkers: false
                })
            };
        }
        
        // Forward the analyzed query to the Worker Dispatcher Lambda
        const dispatcherPayload = {
            correlationId,
            userId,
            originalMessage: message,
            requiredData,
            llmAnalysis: llmResponse.content
        };
        
        const dispatcherResponse = await lambda.invoke({
            FunctionName: CONFIG.workerDispatcherFunction,
            InvocationType: 'RequestResponse', // Synchronous invocation
            Payload: JSON.stringify(dispatcherPayload)
        }).promise();
        
        // Parse and return the dispatcher response
        const responsePayload = JSON.parse(dispatcherResponse.Payload);
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                correlationId,
                message: "Your query is being processed, we're retrieving the necessary information.",
                requiresWorkers: true,
                workersTriggered: responsePayload.workersTriggered
            })
        };
        
    } catch (error) {
        console.error('Error in LLM Query Analyzer Lambda:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: `An error occurred in the LLM Query Analyzer: ${error.message}`,
                status: 'error'
            })
        };
    }
};

/**
 * Call the LLM to analyze the student query and determine required data sources
 * 
 * @param {string} message - The student's question
 * @param {Array} conversationHistory - The conversation history
 * @returns {Object} - The LLM response
 */
async function callLLMForAnalysis(message, conversationHistory) {
    console.log('Calling LLM for analysis with message:', message);
    
    // Get LLM API key from Secrets Manager
    const apiKey = await getApiKeyFromSecrets();
    
    // System prompt to instruct LLM to identify required data sources
    const systemPrompt = `You are an intent recognizer for a student query system. 
    Your task is to analyze student questions about their academic progress and identify 
    what data sources are needed to answer their questions. Available data sources:
    
    - GetStudentDetails: Basic student information (ID, name, enrollment date, etc.)
    - GetStudentCourses: Courses the student is enrolled in or has completed
    - GetProgramDetails: Requirements and structure of academic programs
    - GetCourseDetails: Information about specific courses
    
    Respond with a JSON object that lists the required data sources and any parameters 
    needed for each source. Example response format:
    {
      "requiredData": [
        {"source": "GetStudentDetails", "params": {}},
        {"source": "GetProgramDetails", "params": {"programId": null}}
      ]
    }`;
    
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
                content: 'Now analyze this new question based on the conversation context above:' 
            });
        }
        
        // Add the current query
        messages.push({ role: 'user', content: message });
        
        // Call the LLM API
        const response = await axios.post(
            CONFIG.llmEndpoint,
            {
                model: 'gpt-4',
                messages: messages,
                temperature: 0.3,
                response_format: { type: "json_object" }
            },
            {
                headers: {
                    'Authorization': `Bearer ${apiKey}`,
                    'Content-Type': 'application/json'
                }
            }
        );
        
        return response.data.choices[0].message;
    } catch (error) {
        console.error('Error calling LLM API for analysis:', error);
        throw new Error(`Failed to analyze student query: ${error.message}`);
    }
}

/**
 * Call the LLM for a direct answer when no data sources are needed
 * 
 * @param {string} message - The student's question
 * @param {Array} conversationHistory - The conversation history
 * @returns {Object} - The LLM response
 */
async function callLLMForDirectAnswer(message, conversationHistory) {
    console.log('Calling LLM for direct answer with message:', message);
    
    // Get LLM API key from Secrets Manager
    const apiKey = await getApiKeyFromSecrets();
    
    // System prompt to instruct LLM to provide a direct answer
    const systemPrompt = `You are a helpful academic advisor for university students.
    The following question from a student can be answered directly without needing to fetch additional data.
    Provide a clear, concise, and helpful response based on your knowledge of academic policies and best practices.
    If you cannot answer with certainty, suggest the student contact their academic advisor for more specific information.`;
    
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
                content: 'Now answer this new question based on the conversation context above:' 
            });
        }
        
        // Add the current query
        messages.push({ role: 'user', content: message });
        
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
        
        return response.data.choices[0].message;
    } catch (error) {
        console.error('Error calling LLM API for direct answer:', error);
        
        return {
            content: `I apologize, but I'm having trouble processing your question right now. Please try again or contact academic services if you need immediate assistance.`
        };
    }
}

/**
 * Check if the message is small talk or a general question
 * 
 * @param {string} message - The student's question
 * @param {Array} conversationHistory - The conversation history
 * @returns {Object} - Response indicating if it's small talk and the direct response
 */
async function checkForSmallTalk(message, conversationHistory) {
    console.log('Checking for small talk with message:', message);
    
    // Get LLM API key from Secrets Manager
    const apiKey = await getApiKeyFromSecrets();
    
    // System prompt to instruct LLM to check for small talk
    const systemPrompt = `You are a conversational AI for a student query system that helps students with questions about their academic progress.
    
    Determine if the following message is small talk (greeting, introduction, pleasantry) or a general question that doesn't require specific student data to answer.
    
    Examples of small talk or general questions:
    - "Hello, how are you?"
    - "Can I ask you a question?"
    - "What can you help me with?"
    - "Who created you?"
    - "What's your name?"
    
    Your task:
    1. Determine if the message is small talk or a general question
    2. If it is, provide a friendly, helpful response that explains what you can help with (academic progress, courses, grades, etc.)
    3. If it's not small talk but an academic question that requires student data, indicate that it needs further analysis
    
    You MUST respond in the following JSON format:
    {
      "isSmallTalk": true/false,
      "response": "Your friendly response here if it's small talk, otherwise leave empty"
    }`;
    
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
                content: 'Now check this new question based on the conversation context above:' 
            });
        }
        
        // Add the current query
        messages.push({ role: 'user', content: message });
        
        // Call the LLM API
        const response = await axios.post(
            CONFIG.llmEndpoint,
            {
                model: 'gpt-4',
                messages: messages,
                temperature: 0.3,
                response_format: { type: "json_object" }
            },
            {
                headers: {
                    'Authorization': `Bearer ${apiKey}`,
                    'Content-Type': 'application/json'
                }
            }
        );
        
        const parsedResponse = JSON.parse(response.data.choices[0].message.content);
        
        return {
            isSmallTalk: parsedResponse.isSmallTalk,
            response: parsedResponse.response
        };
    } catch (error) {
        console.error('Error checking for small talk:', error);
        return {
            isSmallTalk: false,
            response: ''
        };
    }
}

/**
 * Store conversation history in DynamoDB
 * 
 * @param {string} userId - The user ID
 * @param {string} correlationId - The correlation ID
 * @param {string} question - The student's question
 * @param {string} answer - The response to the question
 * @returns {Promise} - Promise that resolves when the conversation history is stored
 */
async function storeConversationHistory(userId, correlationId, question, answer) {
    // Set expiration time (15 minutes from now)
    const expirationTime = Math.floor(Date.now() / 1000) + (15 * 60);
    
    const params = {
        TableName: CONFIG.conversationTableName,
        Item: {
            UserId: userId,
            CorrelationId: correlationId,
            Question: question,
            Answer: answer,
            Timestamp: Date.now(),
            ExpirationTime: expirationTime
        }
    };
    
    try {
        await dynamoDB.put(params).promise();
    } catch (error) {
        console.error('Error storing conversation history:', error);
        throw error;
    }
}

/**
 * Parse the LLM response to extract required data sources
 * 
 * @param {Object} llmResponse - The response from the LLM
 * @returns {Array} - Array of required data sources
 */
async function parseRequiredDataSources(llmResponse) {
    try {
        console.log('Parsing LLM response for required data sources:', llmResponse);
        
        // Extract the content from the message
        const content = llmResponse.content;
        
        // Parse the JSON response
        const parsedResponse = JSON.parse(content);
        
        // Return the required data sources
        return parsedResponse.requiredData || [];
    } catch (error) {
        console.error('Error parsing LLM response:', error);
        return [];
    }
}

/**
 * Get the conversation history from DynamoDB
 * 
 * @param {string} userId - The user ID
 * @returns {Array} - The conversation history
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
        ScanIndexForward: false, // Get most recent conversations first
        Limit: 10 // Limit to last 10 interactions to avoid context overflow
    };
    
    try {
        const result = await dynamoDB.query(params).promise();
        const conversations = result.Items || [];
        
        // Format conversation history for LLM context
        return conversations.map(item => ({
            question: item.Question,
            answer: item.Answer,
            timestamp: item.Timestamp
        })).sort((a, b) => a.timestamp - b.timestamp); // Sort by timestamp (oldest first)
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
        
        // Return the API key
        return data.SecretString;
    } catch (error) {
        console.error('Error retrieving API key from Secrets Manager:', error);
        throw new Error('Failed to retrieve LLM API key');
    }
}
