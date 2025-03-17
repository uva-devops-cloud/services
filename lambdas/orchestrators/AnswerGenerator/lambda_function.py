import json
import os
import boto3
from typing import Dict, Any, List
import sys
from datetime import datetime

# Import the agent code
from agent.agent_logic import create_portal_agent
from agent.tools import check_gpa, get_name

# AWS clients
dynamodb = boto3.resource('dynamodb')
secretsmanager = boto3.client('secretsmanager')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Answer Generator Lambda Function
    
    This Lambda function takes aggregated data from worker lambdas and generates
    a final answer using the LLM.
    
    Flow:
    1. Receive aggregated data from Response Aggregator Lambda
    2. Format the data for the LLM
    3. Call the LLM to generate a final answer
    4. Store the final answer in DynamoDB
    5. Return success/failure response
    
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
        
        # Format the data for the LLM
        formatted_data = format_data_for_llm(message, responses)
        
        # Call the LLM for final answer generation
        final_answer = generate_answer(user_id, message, formatted_data)
        
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

def generate_answer(user_id: str, message: str, data: Dict[str, Any]) -> str:
    """
    Generate a final answer using the LLM
    
    Args:
        user_id: Student ID
        message: Original student question
        data: Formatted data from worker lambdas
        
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
        """
        
        # Initialize the LLM
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import SystemMessage, HumanMessage
        
        # Create a one-time LLM instance for this specific task
        model = ChatAnthropic(
            model="claude-3-sonnet-20240229",  # Using a more capable model for final answers
            temperature=0.7,
            max_tokens=2048,
            system=system_prompt,
            api_key=api_key
        )
        
        # Call the LLM
        response = model.invoke([HumanMessage(content=message)])
        
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
        # Get the table name from environment variable
        responses_table_name = os.environ.get('RESPONSES_TABLE_NAME', 'StudentQueryResponses')
        responses_table = dynamodb.Table(responses_table_name)
        
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
