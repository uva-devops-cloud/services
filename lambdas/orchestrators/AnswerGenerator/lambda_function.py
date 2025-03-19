import os
import json
import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS service clients
dynamodb = boto3.resource('dynamodb')
secrets_client = boto3.client('secretsmanager')

# Configuration
CONFIG = {
    'requests_table_name': os.environ.get('REQUESTS_TABLE_NAME', 'StudentQueryRequests'),
    'responses_table_name': os.environ.get('RESPONSES_TABLE_NAME', 'StudentQueryResponses'),
    'conversation_table_name': os.environ.get('CONVERSATION_TABLE_NAME', 'ConversationMemory'),
    'llm_api_key_secret_arn': os.environ.get('LLM_API_KEY_SECRET_ARN'),
    'llm_endpoint': os.environ.get('LLM_ENDPOINT', 'https://api.anthropic.com/v1/messages')
}

# System message for final answer generation
FINAL_ANSWER_SYSTEM_MESSAGE = """
You are a university student portal assistant providing information to students about their academic records.

Your task is to create a helpful, informative, and natural response based on the information provided by various data sources.

Follow these guidelines when generating an answer:

1. Be concise but thorough. Focus on directly addressing the student's question without unnecessary information.
2. Be conversational and friendly, addressing the student directly in your response.
3. Organize information logically and use appropriate formatting to enhance readability.
4. Acknowledge any limitations in the data provided and indicate if additional information may be needed.
5. If the data from different sources appears contradictory, note this in your response and provide the most reliable information.
6. Use consistent terminology regarding academic concepts throughout your response.
7. Maintain a helpful and supportive tone, especially when discussing academic challenges.
8. Include relevant numerical data exactly as provided without rounding or simplification.
9. When appropriate, offer brief suggestions or next steps the student might consider.

REMEMBER: Only provide information about the specific student ID that is implicitly represented in this conversation. Never provide information about other students.
"""


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Answer Generator Lambda Function
    
    This Lambda function takes aggregated data from worker lambdas and generates
    a final answer using the LLM.
    
    Args:
        event: Event from Response Aggregator Lambda containing aggregated worker responses
        context: Lambda context
        
    Returns:
        Dict with success/failure response
    """
    try:
        logger.info(f"Received aggregated data: {json.dumps(event)}")
        
        # Extract data from the event
        correlation_id = event.get('correlationId')
        user_id = event.get('userId')
        message = event.get('message')
        responses = event.get('responses')
        
        if not all([correlation_id, user_id, message, responses]):
            return { 
                'statusCode': 400, 
                'body': json.dumps({
                    'message': 'Missing required data in event',
                    'status': 'error'
                })
            }
        
        # Load conversation history for this user
        conversation_history = get_conversation_history(user_id)
        
        # Format the data for the LLM
        formatted_data = format_data_for_llm(message, responses)
        
        # Call the LLM for final answer generation
        final_answer = generate_answer(message, formatted_data, conversation_history)
        
        # Store the final answer in DynamoDB
        store_response(correlation_id, user_id, message, final_answer)
        
        # Store conversation history
        store_conversation_history(user_id, correlation_id, message, final_answer)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'correlationId': correlation_id,
                'message': final_answer,
                'status': 'complete'
            })
        }
        
    except Exception as e:
        logger.error(f"Error in Answer Generator Lambda: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f"An error occurred in the Answer Generator: {str(e)}",
                'status': 'error'
            })
        }


def get_llm_client() -> ChatAnthropic:
    """
    Creates and returns a configured LLM client
    
    Returns:
        ChatAnthropic instance
    """
    api_key = get_api_key_from_secrets()
    
    return ChatAnthropic(
        model="claude-3-haiku-20240307",
        temperature=0.1,
        anthropic_api_key=api_key,
        max_tokens=1024
    )


def get_api_key_from_secrets() -> str:
    """
    Retrieves the LLM API key from AWS Secrets Manager
    
    Returns:
        API key as string
    """
    try:
        response = secrets_client.get_secret_value(
            SecretId=CONFIG['llm_api_key_secret_arn']
        )
        secret_string = response['SecretString']
        
        # The secret could be a JSON string with the key stored under 'ANTHROPIC_API_KEY'
        try:
            secret_json = json.loads(secret_string)
            if isinstance(secret_json, dict) and 'ANTHROPIC_API_KEY' in secret_json:
                return secret_json['ANTHROPIC_API_KEY']
        except json.JSONDecodeError:
            # If not JSON, assume the secret string is the raw API key
            pass
            
        return secret_string
    except Exception as e:
        logger.error(f"Error retrieving API key from Secrets Manager: {str(e)}")
        raise e


def format_data_for_llm(original_message: str, responses: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format aggregated data for the LLM
    
    Args:
        original_message: Original student question
        responses: Dictionary of responses from worker lambdas
        
    Returns:
        Formatted data for LLM
    """
    logger.info(f"Formatting data with responses type: {type(responses)}")
    formatted_data = {}
    
    # Responses is already a dictionary with source names as keys
    for source_name, response_obj in responses.items():
        logger.info(f"Processing response from {source_name}")
        # Extract data - note the lowercase 'data' key
        data = response_obj.get('data', {})
        formatted_data[source_name] = data
        logger.info(f"Added data from {source_name}, keys: {list(data.keys()) if isinstance(data, dict) else 'list data'}")
    
    return formatted_data


def generate_answer(message: str, data: Dict[str, Any], conversation_history: List[Dict[str, str]]) -> str:
    """
    Generate a final answer using the LLM
    
    Args:
        message: Original student question
        data: Formatted data from worker lambdas
        conversation_history: Previous conversation messages
        
    Returns:
        Final answer from LLM
    """
    try:
        logger.info(f"Generating final answer with data: {json.dumps(data)}")
        
        llm = get_llm_client()
        
        # Format conversation history for context
        conversation_context = format_conversation_history(conversation_history)
        
        # Format the data sources as a string
        data_context = "Data sources:\n"
        for source, source_data in data.items():
            data_context += f"\n{source}:\n"
            if isinstance(source_data, dict):
                for key, value in source_data.items():
                    data_context += f"  {key}: {value}\n"
            elif isinstance(source_data, list):
                for item in source_data:
                    if isinstance(item, dict):
                        for key, value in item.items():
                            data_context += f"  {key}: {value}\n"
                    else:
                        data_context += f"  {item}\n"
            else:
                data_context += f"  {source_data}\n"
        
        # Build the prompt
        messages = [
            SystemMessage(content=FINAL_ANSWER_SYSTEM_MESSAGE)
        ]
        
        # Add conversation history if available
        prompt_content = f"Student question: {message}\n\n{data_context}"
        
        if conversation_context:
            prompt_content = f"Conversation history:\n{conversation_context}\n\n{prompt_content}"
            
        messages.append(HumanMessage(content=prompt_content))
        
        # Get response from LLM
        response = llm.invoke(messages)
        
        logger.info(f"Generated answer: {response.content}")
        return response.content
            
    except Exception as e:
        logger.error(f"Error generating answer: {str(e)}", exc_info=True)
        return f"I'm sorry, but I encountered an error while generating your answer. Please try again later. Error: {str(e)}"


def format_conversation_history(conversation_history: List[Dict[str, str]], limit: int = 3) -> str:
    """
    Formats conversation history for inclusion in prompts
    
    Args:
        conversation_history: The conversation history
        limit: Maximum number of turns to include
        
    Returns:
        Formatted conversation history as string
    """
    if not conversation_history:
        return ""
    
    # Take the most recent conversations, limited by the limit parameter
    recent_history = conversation_history[-limit:]
    
    formatted_history = ""
    for entry in recent_history:
        formatted_history += f"Student: {entry.get('question', '')}\n"
        formatted_history += f"Assistant: {entry.get('answer', '')}\n\n"
    
    return formatted_history.strip()


def get_conversation_history(user_id: str, limit: int = 5) -> List[Dict[str, str]]:
    """
    Retrieves conversation history from DynamoDB
    
    Args:
        user_id: The user ID
        limit: Maximum number of conversation entries to retrieve
        
    Returns:
        List of conversation entries
    """
    try:
        table = dynamodb.Table(CONFIG['conversation_table_name'])
        
        # Query for this user's conversations, sorted by timestamp
        response = table.query(
            IndexName="UserConversationsIndex",
            KeyConditionExpression="UserId = :uid",
            ExpressionAttributeValues={
                ":uid": user_id
            },
            ScanIndexForward=False,  # Sort descending by ExpirationTime (newest first)
            Limit=limit
        )
        
        # Format the response
        history = []
        for item in response.get('Items', []):
            history.append({
                'question': item.get('question', ''),
                'answer': item.get('answer', ''),
                'timestamp': item.get('timestamp', '')
            })
        
        return history
    
    except Exception as e:
        logger.error(f"Error retrieving conversation history: {str(e)}", exc_info=True)
        return []  # Return empty list on error


def store_response(correlation_id: str, user_id: str, question: str, answer: str) -> None:
    """
    Store the final response in DynamoDB
    
    Args:
        correlation_id: Correlation ID for the request
        user_id: User ID
        question: Original question
        answer: Final answer from LLM
    """
    try:
        table = dynamodb.Table(CONFIG['responses_table_name'])
        
        timestamp = datetime.now().isoformat()
        
        item = {
            'CorrelationId': correlation_id,
            'UserId': user_id,
            'Timestamp': timestamp,
            'Question': question,
            'Answer': answer,
            'Status': 'complete'
        }
        
        table.put_item(Item=item)
        logger.info(f"Stored response for correlation ID {correlation_id}")
        
    except Exception as e:
        logger.error(f"Error storing response: {str(e)}", exc_info=True)
        raise e  # Re-raise to be caught by the caller


def store_conversation_history(user_id: str, correlation_id: str, question: str, answer: str) -> None:
    """
    Stores conversation history in DynamoDB
    
    Args:
        user_id: The user ID
        correlation_id: The correlation ID
        question: The student's question
        answer: The response
    """
    try:
        table = dynamodb.Table(CONFIG['conversation_table_name'])
        
        timestamp = datetime.now().isoformat()
        expiration_time = int((datetime.now() + timedelta(minutes=15)).timestamp())
        
        item = {
            'UserId': user_id,  # Correct casing for hash key
            'CorrelationId': correlation_id,  # Correct casing for range key
            'timestamp': timestamp,
            'ExpirationTime': expiration_time,  # Added TTL field
            'question': question,
            'answer': answer
        }
        
        table.put_item(Item=item)
        logger.info(f"Stored conversation history for user {user_id}")
        
    except Exception as e:
        logger.error(f"Error storing conversation history: {str(e)}")
        raise e  # Re-raise to be caught by the caller
