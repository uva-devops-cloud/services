# LLM Query Analyzer - Python Implementation

This Python implementation of the LLM Query Analyzer Lambda function replaces the JavaScript version while maintaining the same functionality within the architecture. It uses the Anthropic Claude model via LangChain for query analysis.

## Key Features

- **Small Talk Detection**: Identifies when a student query is just general conversation vs. requiring database access
- **Intent Recognition**: Analyzes the query to determine required data sources and operations
- **Direct Response**: Provides immediate answers for general questions that don't require database access
- **Conversation History**: Maintains context across multiple interactions

## Lambda Configuration

The Lambda is configured with the following environment variables:

- `WORKER_DISPATCHER_FUNCTION`: Name of the Worker Dispatcher Lambda to invoke
- `LLM_ENDPOINT`: Endpoint for the Anthropic API
- `LLM_API_KEY_SECRET_ARN`: ARN of the secret containing the Anthropic API key
- `CONVERSATION_TABLE_NAME`: Name of the DynamoDB table for conversation history

## Interface

This Lambda maintains the same interface as the JavaScript version, ensuring compatibility with the existing architecture.

### Input Event Structure
```json
{
  "correlationId": "unique-correlation-id",
  "userId": "cognito-user-id",
  "message": "student query text"
}
```

### Output Structure
```json
{
  "statusCode": 200,
  "body": {
    "correlationId": "unique-correlation-id",
    "message": "response text or status message",
    "requiresWorkers": true|false,
    "analysis": { /* query analysis if workers required */ }
  }
}
```

## Deployment

To deploy the Lambda:

1. Run the `build_package.sh` script to create a deployment package
2. Upload the package to AWS Lambda
3. Ensure the environment variables are correctly set

## Note on API Key

The Lambda retrieves the Anthropic API key from AWS Secrets Manager using the ARN specified in the `LLM_API_KEY_SECRET_ARN` environment variable.
