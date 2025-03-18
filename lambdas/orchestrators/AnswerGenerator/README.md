# Answer Generator - Python Implementation

This Python implementation of the Answer Generator Lambda function replaces the JavaScript version while maintaining the same functionality within the architecture. It uses the Anthropic Claude model via LangChain for generating final answers to student queries.

## Key Features

- **Response Generation**: Creates cohesive, natural language responses based on aggregated data from worker Lambdas
- **Context Awareness**: Incorporates conversation history to maintain context across interactions
- **Data Formatting**: Transforms structured data from various sources into a coherent response
- **Persistence**: Stores final answers and conversation history in DynamoDB

## Lambda Configuration

The Lambda is configured with the following environment variables:

- `REQUESTS_TABLE_NAME`: Name of the DynamoDB table for storing student query requests
- `RESPONSES_TABLE_NAME`: Name of the DynamoDB table for storing final answers
- `CONVERSATION_TABLE_NAME`: Name of the DynamoDB table for conversation history
- `LLM_ENDPOINT`: Endpoint for the Anthropic API
- `LLM_API_KEY_SECRET_ARN`: ARN of the secret containing the Anthropic API key

## Interface

This Lambda maintains the same interface as the JavaScript version, ensuring compatibility with the existing architecture.

### Input Event Structure
```json
{
  "correlationId": "unique-correlation-id",
  "userId": "cognito-user-id",
  "message": "student query text",
  "responses": [
    {
      "Source": "source-name",
      "Data": { /* source-specific data */ }
    },
    ...
  ]
}
```

### Output Structure
```json
{
  "statusCode": 200,
  "body": {
    "correlationId": "unique-correlation-id",
    "message": "generated answer text",
    "status": "complete"
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
