const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');

/**
 * Orchestrator Lambda Function
 * 
 * This Lambda function serves as an orchestrator between a React frontend,
 * a Language Model (LLM), and various backend services through EventBridge events.
 * 
 * Flow:
 * 1. Receive request from React frontend
 * 2. Send request to LLM
 * 3. Parse LLM response to determine required actions
 * 4. Send events to EventBridge
 * 5. Wait for responses from triggered Lambda functions
 * 6. Send collected responses back to LLM for final processing
 * 7. Return final response to frontend
 */


// AWS service clients
const eventBridge = new AWS.EventBridge();
const lambda = new AWS.Lambda();
const sqs = new AWS.SQS();

/**
 * AWS Resources Required:
 * - IAM Role: Lambda execution role with permissions for EventBridge, SQS, and other Lambda functions
 * - EventBridge: Event bus for dispatching events to various services
 * - SQS: Queue for collecting responses from triggered services
 * - Lambda: This function and the services it orchestrates
 * - API Gateway: To expose this Lambda to the React frontend
 * - Secrets Manager or Parameter Store: To store LLM API keys
 */

// Configuration (consider moving to environment variables)
const CONFIG = {
    eventBusName: 'OrchestratorEventBus',
    responseQueueUrl: 'https://sqs.region.amazonaws.com/account-id/OrchestratorResponseQueue',
    llmEndpoint: process.env.LLM_ENDPOINT || 'your-llm-endpoint',
    llmApiKey: process.env.LLM_API_KEY,
    responseTimeout: 30000, // 30 seconds to wait for all responses
};

/**
 * Available event types - Add new event types here
 * For each event type, you need to implement a corresponding handler in another Lambda
 * that listens to the event bus for this event type.
 */
const EVENT_TYPES = {
    DATA_RETRIEVAL: 'data-retrieval',
    CALCULATION: 'calculation',
    EXTERNAL_API_CALL: 'external-api-call',
    // Add more event types as needed
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - API Gateway event or direct Lambda invocation
 * @param {Object} context - Lambda context
 * @returns {Object} - Response to be returned to the frontend
 */
exports.handler = async (event, context) => {
    try {
        console.log('Received request:', JSON.stringify(event));
        
        // Extract user message from the event
        const userMessage = extractUserMessage(event);
        
        // Generate a unique correlation ID for tracking this request flow
        const correlationId = uuidv4();
        
        // Step 1: Call the LLM with the user's message
        const llmResponse = await callLLM(userMessage, { isInitialRequest: true });
        
        // Step 2: Parse the LLM response to determine what events to trigger
        const eventsToTrigger = parseEventsFromLLMResponse(llmResponse);
        
        if (eventsToTrigger.length === 0) {
            // If no events needed, return the LLM response directly
            return formatResponse(llmResponse);
        }
        
        // Step 3: Trigger the necessary events and collect the correlation IDs
        const triggeredEvents = await triggerEvents(eventsToTrigger, correlationId);
        
        // Step 4: Wait for responses from all triggered events
        const eventResponses = await waitForResponses(correlationId, triggeredEvents.length);
        
        // Step 5: Call the LLM again with the collected responses
        const finalResponse = await callLLM(userMessage, {
            isInitialRequest: false,
            eventResponses
        });
        
        // Step 6: Return the final response to the frontend
        return formatResponse(finalResponse);
        
    } catch (error) {
        console.error('Error in orchestrator:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'An error occurred in the orchestrator',
                error: error.message
            })
        };
    }
};

/**
 * Extract the user message from the incoming event
 * 
 * @param {Object} event - The Lambda event
 * @returns {string} - The extracted user message
 */
function extractUserMessage(event) {
    // Adjust based on your actual event structure
    if (event.body) {
        // API Gateway event
        const body = typeof event.body === 'string' ? JSON.parse(event.body) : event.body;
        return body.message;
    }
    // Direct Lambda invocation
    return event.message;
}

/**
 * Call the LLM service
 * 
 * AWS Resources Required:
 * - Secrets Manager: To securely store the LLM API key
 * - VPC Endpoint or Internet Gateway: For the Lambda to reach the LLM API
 * - IAM: Permission to access Secrets Manager and make HTTP calls
 *
 * @param {string} userMessage - The user's message
 * @param {Object} options - Additional options for the LLM call
 * @returns {Object} - The LLM response
 */
async function callLLM(userMessage, options) {
    // TODO: Implement the actual API call to your LLM provider
    console.log('Calling LLM with message:', userMessage);
    console.log('Options:', options);
    
    // This is a placeholder. Replace with actual LLM API call.
    // For example, if using OpenAI:
    /*
    const response = await axios.post(
        CONFIG.llmEndpoint,
        {
            model: 'gpt-4',
            messages: [
                { role: 'system', content: 'You are a helpful assistant.' },
                { role: 'user', content: userMessage },
                ... // Add any context from previous responses if not initial request
            ],
            temperature: 0.7
        },
        {
            headers: {
                'Authorization': `Bearer ${CONFIG.llmApiKey}`,
                'Content-Type': 'application/json'
            }
        }
    );
    return response.data;
    */
    
    // Simulated response for testing
    return {
        content: options.isInitialRequest
            ? "I'll need to retrieve some data and do a calculation. [EVENT:data-retrieval] [EVENT:calculation]"
            : "Based on the retrieved data and calculation, here's your answer..."
    };
}

/**
 * Parse the LLM response to determine what events to trigger
 * 
 * @param {Object} llmResponse - The response from the LLM
 * @returns {Array} - Array of event configurations to trigger
 */
function parseEventsFromLLMResponse(llmResponse) {
    const content = llmResponse.content;
    const eventConfigs = [];
    
    // Look for event markers in the LLM response
    // Assuming the LLM uses [EVENT:type] syntax to indicate needed events
    const eventRegex = /\[EVENT:([a-z-]+)\]/g;
    let match;
    
    while ((match = eventRegex.exec(content)) !== null) {
        const eventType = match[1];
        if (EVENT_TYPES[eventType.toUpperCase().replace(/-/g, '_')]) {
            eventConfigs.push({
                eventType,
                // Add any additional parameters parsed from the LLM response
                params: {}
            });
        }
    }
    
    return eventConfigs;
}

/**
 * Trigger events on the EventBridge event bus
 * 
 * AWS Resources Required:
 * - EventBridge: Event bus for dispatching events
 * - IAM: Permission for this Lambda to put events on the event bus
 * - Lambda: Target Lambda functions that process these events
 * - EventBridge Rules: To route events to the appropriate Lambda functions
 *
 * @param {Array} events - Events to trigger
 * @param {string} correlationId - ID to correlate requests and responses
 * @returns {Array} - Information about triggered events
 */
async function triggerEvents(events, correlationId) {
    const triggeredEvents = [];
    
    for (const event of events) {
        // Construct the event
        const eventBridgeEvent = {
            Entries: [{
                Source: 'orchestrator.lambda',
                DetailType: event.eventType,
                Detail: JSON.stringify({
                    correlationId,
                    params: event.params
                }),
                EventBusName: CONFIG.eventBusName
            }]
        };
        
        console.log(`Triggering event ${event.eventType}:`, eventBridgeEvent);
        
        try {
            await eventBridge.putEvents(eventBridgeEvent).promise();
            triggeredEvents.push({
                eventType: event.eventType,
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            console.error(`Error triggering event ${event.eventType}:`, error);
            // Continue with other events even if one fails
        }
    }
    
    return triggeredEvents;
}

/**
 * Wait for responses from triggered events
 * 
 * AWS Resources Required:
 * - SQS: Queue where event processors post their responses
 * - IAM: Permission for this Lambda to read from SQS
 * - Lambda: Configure event processor Lambdas to write to this SQS queue
 *
 * @param {string} correlationId - ID to correlate requests and responses
 * @param {number} expectedResponses - Number of responses to wait for
 * @returns {Array} - Collected responses
 */
async function waitForResponses(correlationId, expectedResponses) {
    const responses = [];
    const startTime = Date.now();
    
    while (responses.length < expectedResponses && 
                 Date.now() - startTime < CONFIG.responseTimeout) {
        
        // Poll SQS for messages
        const result = await sqs.receiveMessage({
            QueueUrl: CONFIG.responseQueueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 5, // Long polling
            MessageAttributeNames: ['All']
        }).promise();
        
        if (!result.Messages || result.Messages.length === 0) {
            continue;
        }
        
        for (const message of result.Messages) {
            try {
                const body = JSON.parse(message.Body);
                
                // Check if this message is for our correlation ID
                if (body.correlationId === correlationId) {
                    responses.push(body);
                    
                    // Delete the message from the queue
                    await sqs.deleteMessage({
                        QueueUrl: CONFIG.responseQueueUrl,
                        ReceiptHandle: message.ReceiptHandle
                    }).promise();
                }
            } catch (error) {
                console.error('Error processing SQS message:', error);
            }
        }
    }
    
    return responses;
}

/**
 * Format the final response to return to the frontend
 *
 * @param {Object} llmResponse - The final LLM response
 * @returns {Object} - Formatted response for the frontend
 */
function formatResponse(llmResponse) {
    // Clean up any event markers or other internal syntax from the response
    let cleanResponse = llmResponse.content;
    cleanResponse = cleanResponse.replace(/\[EVENT:[a-z-]+\]/g, '');
    
    return {
        statusCode: 200,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*' // Configure CORS as needed
        },
        body: JSON.stringify({
            message: cleanResponse.trim()
        })
    };
}

/**
 * EXTENDING THIS ORCHESTRATOR
 * 
 * To add a new event type:
 * 1. Add it to the EVENT_TYPES constant
 * 2. Create a new Lambda function to handle that event type
 * 3. Create an EventBridge rule to route events of that type to your new Lambda
 * 4. Configure your new Lambda to send its response to the SQS response queue
 * 
 * To modify the LLM interaction:
 * 1. Update the callLLM function with your specific LLM provider's API
 * 2. Adjust the prompt engineering in that function
 * 
 * To change how events are parsed from LLM responses:
 * 1. Modify the parseEventsFromLLMResponse function
 */