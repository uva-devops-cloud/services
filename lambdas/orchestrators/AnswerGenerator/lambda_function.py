import json
import os
import boto3
from typing import Dict, Any, List
import sys
from datetime import datetime
import time

# Import the agent code
from agent.agent_logic import create_portal_agent
from agent.tools import check_gpa, get_name

# AWS clients
dynamodb = boto3.resource('dynamodb')
requests_table = dynamodb.Table(os.environ.get('REQUESTS_TABLE_NAME', 'StudentQueryRequests'))
responses_table = dynamodb.Table(os.environ.get('RESPONSES_TABLE_NAME', 'StudentQueryResponses'))
conversation_table = dynamodb.Table(os.environ.get('CONVERSATION_TABLE_NAME', 'ConversationMemory'))
secretsmanager = boto3.client('secretsmanager')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Answer Generator Lambda Function
    
    This Lambda function takes aggregated data from worker lambdas and generates
    a final answer using the LLM.
    
    Flow:
    1. Receive aggregated data from Response Aggregator Lambda
    2. Load conversation history from DynamoDB
    3. Format the data for the LLM
    4. Call the LLM to generate a final answer
    5. Update conversation history with the answer
    6. Store the final answer in DynamoDB
    7. Return success/failure response
    
    Args:
        event: Event from Response Aggregator Lambda
        context: Lambda context
        
    Returns:
        Dict with success/failure response
    """
    try:
        print(f"Received aggregated data: {json.dumps(event)}")
        
        # Extract data from the event
        correlation_id = event.get('correlationId')
        user_id = event.get('userId')
        message = event.get('message')
        responses = event.get('responses')
        
        if not correlation_id or not message or not responses:
            return { 
                'statusCode': 400, 
                'body': json.dumps({
                    'message': 'Missing required data in event',
                    'status': 'error'
                })
            }
        
        # Load conversation history for this user
        conversation_history = load_conversation_history(user_id, correlation_id)
        
        # Format the data for the LLM
        formatted_data = format_data_for_llm(message, responses)
        
        # Call the LLM for final answer generation
        final_answer = generate_answer(user_id, message, formatted_data, conversation_history)
        
        # Update conversation history with the answer
        update_conversation_history(user_id, correlation_id, "assistant", final_answer)
        
        # Store the final answer in DynamoDB
        store_response(correlation_id, user_id, message, final_answer)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'correlationId': correlation_id,
                'message': 'Answer generated successfully',
                'status': 'complete',
                'answer': final_answer
            })
        }
        
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error generating answer: {str(e)}',
                'status': 'error'
            })
        }

def load_conversation_history(user_id: str, correlation_id: str) -> List[Dict[str, str]]:
    """
    Load conversation history from DynamoDB
    
    Args:
        user_id: Student ID
        correlation_id: Correlation ID for the current conversation
        
    Returns:
        List of conversation messages
    """
    try:
        # Query DynamoDB for conversation history
        response = conversation_table.query(
            KeyConditionExpression="UserId = :userId AND CorrelationId = :correlationId",
            ExpressionAttributeValues={
                ":userId": user_id,
                ":correlationId": correlation_id
            }
        )
        
        # If no history found, return empty list
        if not response.get('Items'):
            return []
        
        # Sort messages by timestamp
        messages = response.get('Items', [])
        messages.sort(key=lambda x: x.get('Timestamp', ''))
        
        # Format messages for LLM
        formatted_messages = []
        for msg in messages:
            formatted_messages.append({
                'role': msg.get('Role'),
                'content': msg.get('Content')
            })
        
        return formatted_messages
        
    except Exception as e:
        print(f"Error loading conversation history: {str(e)}")
        return []

def update_conversation_history(user_id: str, correlation_id: str, role: str, content: str) -> None:
    """
    Update conversation history in DynamoDB
    
    Args:
        user_id: Student ID
        correlation_id: Correlation ID for the current conversation
        role: Message role (user or assistant)
        content: Message content
    """
    try:
        # Calculate TTL (15 minutes from now)
        ttl = int(time.time()) + (15 * 60)
        timestamp = int(time.time() * 1000)  # millisecond timestamp for sorting
        
        # Store message in DynamoDB
        conversation_table.put_item(
            Item={
                'UserId': user_id,
                'CorrelationId': correlation_id,
                'MessageId': f"{timestamp}",
                'Timestamp': timestamp,
                'Role': role,
                'Content': content,
                'ExpirationTime': ttl
            }
        )
        
    except Exception as e:
        print(f"Error updating conversation history: {str(e)}")

def format_data_for_llm(original_message: str, responses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Format aggregated data for the LLM
    
    Args:
        original_message: Original student question
        responses: Responses from worker lambdas
        
    Returns:
        Formatted data for LLM
    """
    formatted_data = {}
    
    for response in responses:
        source = response.get('source')
        data = response.get('data')
        
        if source and data:
            formatted_data[source] = data
    
    return formatted_data

def generate_answer(user_id: str, message: str, data: Dict[str, Any], conversation_history: List[Dict[str, str]]) -> str:
    """
    Generate a final answer using the LLM
    
    Args:
        user_id: Student ID
        message: Original student question
        data: Formatted data from worker lambdas
        conversation_history: Previous conversation messages
        
    Returns:
        Final answer from LLM
    """
    try:
        # Get API key from Secrets Manager
        api_key = get_api_key_from_secrets()
        
        # Create a custom system prompt for answer generation
        system_prompt = f"""You are a helpful academic advisor for university students.
        Use the following data to answer the student's question clearly and concisely.
        Make sure to reference specific information from the provided data to support your answer.
        Do not invent any information not included in the data.
        
        Available data: {json.dumps(data, indent=2)}
        
        Student ID: {user_id}
        
        Remember to be helpful, accurate, and concise in your response.
        If the student is asking a follow-up question, refer to the conversation history to maintain context.
        """
        
        # Initialize the LLM
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
        
        # Create a one-time LLM instance for this specific task
        model = ChatAnthropic(
            model="claude-3-sonnet-20240229",  # Using a more capable model for final answers
            temperature=0.7,
            max_tokens=2048,
            system=system_prompt,
            api_key=api_key
        )
        
        # Prepare messages including conversation history
        messages = []
        for msg in conversation_history:
            if msg['role'] == 'user':
                messages.append(HumanMessage(content=msg['content']))
            elif msg['role'] == 'assistant':
                messages.append(AIMessage(content=msg['content']))
        
        # Add the current message if it's not already in the history
        if not conversation_history or conversation_history[-1]['role'] != 'user' or conversation_history[-1]['content'] != message:
            messages.append(HumanMessage(content=message))
        
        # Call the LLM
        response = model.invoke(messages)
        
        return response.content
        
    except Exception as e:
        print(f"Error generating answer: {str(e)}")
        return f"I've analyzed your academic data regarding: '{message}'. However, I encountered an issue generating a detailed response. Please try again or contact academic services for assistance."

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
        # Generate a timestamp for the response
        timestamp = datetime.utcnow().isoformat()
        
        # Store the response in DynamoDB
        responses_table.put_item(
            Item={
                'CorrelationId': correlation_id,
                'Timestamp': timestamp,
                'UserId': user_id,
                'Question': question,
                'Answer': answer,
                'TTL': int(datetime.utcnow().timestamp()) + (30 * 24 * 60 * 60)  # 30 days TTL
            }
        )
        
    except Exception as e:
        print(f"Error storing response in DynamoDB: {str(e)}")
        # We don't want to fail the entire function if just the storage fails
        # Just log the error and continue

def get_api_key_from_secrets() -> str:
    """
    Get the LLM API key from AWS Secrets Manager
    
    Returns:
        The API key
    """
    try:
        secret_arn = os.environ.get('LLM_API_KEY_SECRET_ARN')
        if not secret_arn:
            # For local testing, try to load from .env
            from dotenv import load_dotenv
            load_dotenv()
            return os.getenv('ANTHROPIC_API_KEY', 'dummy-key-for-testing')
            
        response = secretsmanager.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response['SecretString'])
        return secret.get('ANTHROPIC_API_KEY', '')
    except Exception as e:
        print(f"Error retrieving API key from Secrets Manager: {str(e)}")
        # For testing only - never use dummy keys in production
        return 'dummy-key-for-testing'
