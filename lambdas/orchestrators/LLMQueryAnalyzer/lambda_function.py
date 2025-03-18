import os
import json
import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS service clients
lambda_client = boto3.client('lambda')
secrets_client = boto3.client('secretsmanager')
dynamodb = boto3.resource('dynamodb')

# Configuration
CONFIG = {
    'worker_dispatcher_function': os.environ.get('WORKER_DISPATCHER_FUNCTION', 'student-query-worker-dispatcher'),
    'conversation_table_name': os.environ.get('CONVERSATION_TABLE_NAME', 'ConversationMemory'),
    'llm_api_key_secret_arn': os.environ.get('LLM_API_KEY_SECRET_ARN'),
    'llm_endpoint': os.environ.get('LLM_ENDPOINT', 'https://api.anthropic.com/v1/messages')
}

# System messages
SMALL_TALK_SYSTEM_MESSAGE = """
You are a friendly and helpful university assistant, responding to students about their academic questions. 
Determine if the student's message is small talk (greetings, thank you, general conversation) 
or a general question that doesn't require querying a database.

Respond with a JSON object with these fields:
- "isSmallTalk": true if the message is small talk or a general question, false if it requires database access
- "response": your helpful direct response if it's small talk, empty string otherwise
- "explanation": brief reason for your classification

Examples of small talk or general questions:
- "Hello there"
- "How are you today?"
- "Thank you for your help"
- "What services does the university offer?"
- "When is the library open?"
- "Can you explain how the grading system works?"

Examples that require database access (not small talk):
- "What's my current GPA?"
- "Which courses am I enrolled in?"
- "Do I have any outstanding fees?"
- "What grades did I get last semester?"
- "Am I on track to graduate?"
"""

QUERY_ANALYSIS_SYSTEM_MESSAGE = """
You are a specialized AI designed to analyze student queries about their academic records.
Your task is to determine what data sources and operations are needed to answer the query.

Analyze the user's query and output a JSON object with the following fields:
- "dataSources": an array of required data sources ("courses", "grades", "enrollment", "financial", "academic_calendar", etc.)
- "operations": an array of operations needed ("retrieve", "calculate", "compare", "summarize")
- "parameters": key parameters relevant to the query (course_id, semester, year, etc.)
- "question_type": classification of question ("status", "performance", "requirements", "schedule")
- "complexity": estimated complexity ("simple", "moderate", "complex")

If you are completely confident you can answer without any data retrieval, return an empty dataSources array.
"""

DIRECT_ANSWER_SYSTEM_MESSAGE = """
You are a friendly university assistant helping students with general academic questions.
The student doesn't need specific data retrieval for this question. Answer it directly in a helpful, conversational tone.
Provide accurate information while being concise. If you're uncertain about specific university details, 
stick to general academic knowledge and avoid making up specific policies.
"""


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function. Analyzes student queries using LLM and determines 
    if they require data retrieval or can be answered directly.
    
    Args:
        event: Event from Query Intake Lambda
        context: Lambda context
        
    Returns:
        Response with LLM analysis or direct answer
    """
    try:
        logger.info(f"Received request from Query Intake: {json.dumps(event)}")
        
        # Extract information from the event
        correlation_id = event.get('correlationId')
        user_id = event.get('userId')
        message = event.get('message')
        
        if not all([correlation_id, user_id, message]):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters',
                    'status': 'error'
                })
            }
        
        # Retrieve conversation history
        conversation_history = get_conversation_history(user_id)
        
        # Check if this is small talk
        small_talk_response = check_for_small_talk(message, conversation_history)
        
        if small_talk_response['isSmallTalk']:
            logger.info("Small talk detected, providing direct response")
            
            # Store this conversation entry
            try:
                store_conversation_history(user_id, correlation_id, message, 
                                          small_talk_response['response'])
            except Exception as e:
                logger.error(f"Error storing conversation history: {str(e)}")
                # Continue even if storage fails
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'correlationId': correlation_id,
                    'message': small_talk_response['response'],
                    'requiresWorkers': False,
                    'isSmallTalk': True
                })
            }
        
        # Not small talk, continue with regular flow
        # Analyze the query to determine required data sources
        query_analysis = analyze_query(message, conversation_history)
        
        # Parse the required data sources
        required_data = query_analysis.get('dataSources', [])
        
        if not required_data:  # No data sources needed
            # Get direct answer
            direct_response = get_direct_answer(message, conversation_history)
            
            # Store this conversation entry
            try:
                store_conversation_history(user_id, correlation_id, 
                                          message, direct_response)
            except Exception as e:
                logger.error(f"Error storing conversation history: {str(e)}")
                # Continue even if storage fails
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'correlationId': correlation_id,
                    'message': direct_response,
                    'requiresWorkers': False
                })
            }
        
        # Data retrieval required, invoke Worker Dispatcher
        dispatcher_payload = {
            'correlationId': correlation_id,
            'userId': user_id,
            'message': message,
            'analysis': query_analysis
        }
        
        logger.info(f"Invoking Worker Dispatcher with payload: {json.dumps(dispatcher_payload)}")
        
        lambda_response = lambda_client.invoke(
            FunctionName=CONFIG['worker_dispatcher_function'],
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(dispatcher_payload)
        )
        
        return {
            'statusCode': 202,
            'body': json.dumps({
                'correlationId': correlation_id,
                'message': 'Query analysis complete, worker dispatcher invoked',
                'analysis': query_analysis,
                'requiresWorkers': True
            })
        }
        
    except Exception as e:
        logger.error(f"Error in LLM Query Analyzer: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f"An error occurred: {str(e)}",
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
    
    # Add debug logging to help troubleshoot
    logger.info(f"Initializing Anthropic client with API key length: {len(api_key)}")
    logger.info(f"API key prefix: {api_key[:5]}...")
    
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
        
        # Get the raw secret string
        secret_string = response['SecretString']
        
        # Try to parse as JSON if it's in that format
        try:
            secret_json = json.loads(secret_string)
            # If it's a JSON with a known key structure
            if isinstance(secret_json, dict):
                if 'apiKey' in secret_json:
                    return secret_json['apiKey']
                elif 'api_key' in secret_json:
                    return secret_json['api_key']
                # If JSON but without expected key, use first string value found
                for key, value in secret_json.items():
                    if isinstance(value, str) and value.startswith('sk-'):
                        return value
        except json.JSONDecodeError:
            # Not JSON, check if it's a raw API key string
            if secret_string.strip().startswith('sk-'):
                return secret_string.strip()
        
        # If all else fails, return as is
        return secret_string
    except Exception as e:
        logger.error(f"Error retrieving API key from Secrets Manager: {str(e)}")
        raise e

def check_for_small_talk(message: str, conversation_history: List[Dict]) -> Dict:
    """
    Determines if the message is small talk or a general question
    
    Args:
        message: The student's question
        conversation_history: Previous conversations
        
    Returns:
        Dict with isSmallTalk flag and response if applicable
    """
    try:
        llm = get_llm_client()
        
        # Format recent conversation history
        recent_history = format_conversation_history_for_prompt(conversation_history, limit=3)
        
        # Build the prompt with system message and conversation history
        messages = [
            SystemMessage(content=SMALL_TALK_SYSTEM_MESSAGE)
        ]
        
        # Add conversation history if available
        if recent_history:
            messages.append(HumanMessage(
                content=f"Recent conversation: {recent_history}\n\nCurrent message: {message}"
            ))
        else:
            messages.append(HumanMessage(content=message))
        
        # Get response from LLM
        response = llm.invoke(messages)
        
        # Extract and parse the JSON response
        try:
            parsed_response = json.loads(response.content)
            logger.info(f"Small talk check result: {json.dumps(parsed_response)}")
            return parsed_response
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM response as JSON: {response.content}")
            # Fallback response if JSON parsing fails
            return {
                'isSmallTalk': False,
                'response': '',
                'explanation': 'Failed to parse response'
            }
            
    except Exception as e:
        logger.error(f"Error checking for small talk: {str(e)}")
        # Safe fallback
        return {
            'isSmallTalk': False,
            'response': '',
            'explanation': f"Error: {str(e)}"
        }


def analyze_query(message: str, conversation_history: List[Dict]) -> Dict:
    """
    Sends the query to the LLM to determine required data sources
    
    Args:
        message: The student's question
        conversation_history: Previous conversations
        
    Returns:
        Dict with required data sources and query analysis
    """
    try:
        llm = get_llm_client()
        
        # Format recent conversation history
        recent_history = format_conversation_history_for_prompt(conversation_history, limit=3)
        
        # Build the prompt
        messages = [
            SystemMessage(content=QUERY_ANALYSIS_SYSTEM_MESSAGE)
        ]
        
        # Add conversation history if available
        if recent_history:
            messages.append(HumanMessage(
                content=f"Recent conversation: {recent_history}\n\nCurrent query: {message}"
            ))
        else:
            messages.append(HumanMessage(content=message))
        
        # Get response from LLM
        response = llm.invoke(messages)
        
        # Extract and parse the JSON response
        try:
            return json.loads(response.content)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM response as JSON: {response.content}")
            # Fallback analysis if JSON parsing fails
            return {
                'dataSources': ['unknown'],
                'operations': ['retrieve'],
                'parameters': {},
                'question_type': 'unknown',
                'complexity': 'unknown'
            }
            
    except Exception as e:
        logger.error(f"Error analyzing query: {str(e)}")
        # Safe fallback
        return {
            'dataSources': ['error'],
            'operations': ['retrieve'],
            'parameters': {},
            'question_type': 'error',
            'complexity': 'unknown'
        }


def get_direct_answer(message: str, conversation_history: List[Dict]) -> str:
    """
    Gets a direct answer from the LLM when no data sources are needed
    
    Args:
        message: The student's question
        conversation_history: Previous conversations
        
    Returns:
        Direct answer from the LLM
    """
    try:
        llm = get_llm_client()
        
        # Format recent conversation history
        recent_history = format_conversation_history_for_prompt(conversation_history, limit=3)
        
        # Build the prompt
        messages = [
            SystemMessage(content=DIRECT_ANSWER_SYSTEM_MESSAGE)
        ]
        
        # Add conversation history if available
        if recent_history:
            messages.append(HumanMessage(
                content=f"Recent conversation: {recent_history}\n\nStudent question: {message}"
            ))
        else:
            messages.append(HumanMessage(content=f"Student question: {message}"))
        
        # Get response from LLM
        response = llm.invoke(messages)
        
        return response.content
            
    except Exception as e:
        logger.error(f"Error getting direct answer: {str(e)}")
        return "I'm sorry, I'm having trouble responding right now. Please try again later."


def format_conversation_history_for_prompt(conversation_history: List[Dict], limit: int = 3) -> str:
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
        formatted_history += f"Student: {entry['question']}\n"
        formatted_history += f"Assistant: {entry['answer']}\n\n"
    
    return formatted_history.strip()


def get_conversation_history(user_id: str) -> List[Dict]:
    """
    Retrieves conversation history from DynamoDB
    
    Args:
        user_id: The user ID
        
    Returns:
        List of conversation entries
    """
    try:
        table = dynamodb.Table(CONFIG['conversation_table_name'])
        
        # Get records from the last 24 hours
        timestamp_24h_ago = (datetime.now() - timedelta(days=1)).isoformat()
        
        response = table.query(
            KeyConditionExpression='user_id = :uid AND #ts > :ts',
            ExpressionAttributeValues={
                ':uid': user_id,
                ':ts': timestamp_24h_ago
            },
            ExpressionAttributeNames={
                '#ts': 'timestamp'  # Escape the reserved keyword
            },
            ScanIndexForward=True  # Sort by timestamp ascending
        )
        
        # Convert to a simple list of question/answer pairs
        history = []
        for item in response.get('Items', []):
            history.append({
                'question': item.get('question', ''),
                'answer': item.get('answer', ''),
                'timestamp': item.get('timestamp', '')
            })
        
        return history
        
    except Exception as e:
        logger.error(f"Error retrieving conversation history: {str(e)}")
        return []  # Return empty list on error


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
        
        item = {
            'user_id': user_id,
            'timestamp': timestamp,
            'correlation_id': correlation_id,
            'question': question,
            'answer': answer
        }
        
        table.put_item(Item=item)
        logger.info(f"Stored conversation history for user {user_id}")
        
    except Exception as e:
        logger.error(f"Error storing conversation history: {str(e)}")
        raise e  # Re-raise to be caught by the caller
