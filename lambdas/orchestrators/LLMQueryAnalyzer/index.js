const AWS = require('aws-sdk');
const axios = require('axios');

/**
 * LLM Query Analyzer Lambda Function
 * 
 * This Lambda function sends student queries to the LLM for analysis and intent detection.
 * It determines which worker lambdas need to be invoked based on the question.
 * 
 * Flow:
 * 1. Receive student query from Query Intake Lambda
 * 2. Send the query to LLM with system instructions for intent recognition
 * 3. Parse the LLM response to identify required data and worker lambdas
 * 4. Forward structured analysis to Worker Dispatcher Lambda
 */

// AWS service clients
const lambda = new AWS.Lambda();
const secretsManager = new AWS.SecretsManager();

// Configuration
const CONFIG = {
    llmEndpoint: process.env.LLM_ENDPOINT || 'your-llm-endpoint',
    llmApiKeySecretArn: process.env.LLM_API_KEY_SECRET_ARN,
    workerDispatcherFunction: process.env.WORKER_DISPATCHER_FUNCTION || 'student-query-worker-dispatcher'
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
        
        // Call the LLM for query analysis
        const llmResponse = await callLLMForAnalysis(message);
        
        // Parse the LLM response to determine required data sources
        const requiredData = parseRequiredDataSources(llmResponse);
        
        if (requiredData.length === 0) {
            // If no data sources needed, we can directly answer the question
            const directResponse = await callLLMForDirectAnswer(message);
            
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
 * @returns {Object} - The LLM response
 */
async function callLLMForAnalysis(message) {
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
        // This is a placeholder. Replace with actual LLM API call.
        // For example, if using OpenAI:
        const response = await axios.post(
            CONFIG.llmEndpoint,
            {
                model: 'gpt-4',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: message }
                ],
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
        
        return {
            content: response.data.choices[0].message.content
        };
    } catch (error) {
        console.error('Error calling LLM API:', error);
        
        // Simulated response for testing if LLM call fails
        return {
            content: JSON.stringify({
                requiredData: [
                    { source: "GetStudentDetails", params: {} },
                    { source: "GetStudentCourses", params: {} }
                ]
            })
        };
    }
}

/**
 * Call the LLM for a direct answer when no data sources are needed
 * 
 * @param {string} message - The student's question
 * @returns {Object} - The LLM response
 */
async function callLLMForDirectAnswer(message) {
    console.log('Calling LLM for direct answer with message:', message);
    
    // Get LLM API key from Secrets Manager
    const apiKey = await getApiKeyFromSecrets();
    
    const systemPrompt = `You are a helpful academic advisor for university students.
    Answer the student's question clearly and concisely based on your general knowledge.
    If you need specific information about the student's records to provide an accurate answer,
    indicate that more information is needed.`;
    
    try {
        // This is a placeholder. Replace with actual LLM API call.
        const response = await axios.post(
            CONFIG.llmEndpoint,
            {
                model: 'gpt-4',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: message }
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
        
        return {
            content: response.data.choices[0].message.content
        };
    } catch (error) {
        console.error('Error calling LLM API for direct answer:', error);
        
        // Simulated response for testing if LLM call fails
        return {
            content: "I'd be happy to help with your question, but I need more specific information about your academic records to provide an accurate answer. Could you please clarify or provide more details?"
        };
    }
}

/**
 * Parse the LLM response to extract required data sources
 * 
 * @param {Object} llmResponse - The response from the LLM
 * @returns {Array} - Array of required data sources
 */
function parseRequiredDataSources(llmResponse) {
    try {
        const parsed = typeof llmResponse.content === 'string' 
            ? JSON.parse(llmResponse.content) 
            : llmResponse.content;
        
        return parsed.requiredData || [];
    } catch (error) {
        console.error('Error parsing LLM response:', error);
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
