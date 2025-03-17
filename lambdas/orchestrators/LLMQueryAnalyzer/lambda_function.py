import json
import os
import boto3
from typing import Dict, Any, List
import sys

# Import the agent code 
from agent.agent_logic import create_portal_agent
from agent.tools import check_gpa, get_name

# AWS clients
lambda_client = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb').Table(os.environ.get('REQUESTS_TABLE_NAME', 'StudentQueryRequests'))
secretsmanager = boto3.client('secretsmanager')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    LLM Query Analyzer Lambda Function
    
    This Lambda function sends student queries to the LLM for analysis and intent detection.
    It determines which worker lambdas need to be invoked based on the question.
    
    Flow:
    1. Receive student query from Query Intake Lambda
    2. Send the query to LLM with system instructions for intent recognition
    3. Parse the LLM response to identify required data and worker lambdas
    4. Forward structured analysis to Worker Dispatcher Lambda
    
    Args:
        event: Event from Query Intake Lambda
        context: Lambda context
        
    Returns:
        Dict with LLM analysis results
    """
    try:
        print(f"Received request from Query Intake: {json.dumps(event)}")
        
        # Extract the required information from the event
        correlation_id = event.get('correlationId')
        user_id = event.get('userId')
        message = event.get('message')
        
        if not correlation_id or not user_id or not message:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters',
                    'status': 'error'
                })
            }
        
        # Call the LLM for query analysis
        required_data = analyze_query(user_id, message)
        
        if not required_data:
            # If no data sources needed, we can directly answer the question
            direct_response = generate_direct_answer(user_id, message)
            
            return {
                'correlationId': correlation_id,
                'userId': user_id,
                'message': message,
                'directResponse': direct_response,
                'requiredData': []
            }
        
        # Return the analysis results
        return {
            'correlationId': correlation_id,
            'userId': user_id,
            'message': message,
            'requiredData': required_data
        }
        
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error processing request: {str(e)}',
                'status': 'error'
            })
        }

def analyze_query(user_id: str, message: str) -> List[Dict[str, Any]]:
    """
    Analyze the student query to determine required data sources
    
    Args:
        user_id: Student ID
        message: The student's question
        
    Returns:
        List of required data sources
    """
    try:
        # Create a custom system prompt for intent detection
        system_prompt = """You are an intent recognizer for a student query system. 
        Your task is to analyze student questions about their academic progress and identify 
        what data sources are needed to answer their questions. Available data sources:
        
        - GetStudentData: Basic student information (ID, name, enrollment date, etc.)
        - GetStudentCourses: Courses the student is enrolled in or has completed
        - GetStudentCurrentDegree: Information about the student's current degree program
        - GetProgramDetails: Requirements and structure of academic programs
        
        Respond with a JSON object that lists the required data sources. Example response format:
        {
          "requiredData": [
            {"source": "GetStudentData", "params": {}},
            {"source": "GetProgramDetails", "params": {}}
          ]
        }
        
        If the question can be answered directly without any data sources, respond with:
        {
          "requiredData": []
        }
        """
        
        # Initialize the agent with a custom system prompt
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import SystemMessage, HumanMessage
        
        # Get API key from Secrets Manager
        api_key = get_api_key_from_secrets()
        
        # Create a one-time LLM instance for this specific task
        model = ChatAnthropic(
            model="claude-3-haiku-20240307",
            temperature=0.3,
            max_tokens=1024,
            system=system_prompt,
            api_key=api_key
        )
        
        # Format the message with student ID context
        context_prompt = f"Student id: {user_id}. {message}"
        
        # Call the LLM
        response = model.invoke([HumanMessage(content=context_prompt)])
        
        # Parse the response to extract required data sources
        try:
            # Try to parse as JSON
            content = response.content
            if isinstance(content, str):
                result = json.loads(content)
                return result.get('requiredData', [])
            return []
        except json.JSONDecodeError:
            print(f"Failed to parse LLM response as JSON: {response.content}")
            # Fallback to default data sources if parsing fails
            return [
                {"source": "GetStudentData", "params": {}},
                {"source": "GetStudentCourses", "params": {}}
            ]
            
    except Exception as e:
        print(f"Error analyzing query: {str(e)}")
        # Fallback to default data sources if an error occurs
        return [
            {"source": "GetStudentData", "params": {}},
            {"source": "GetStudentCourses", "params": {}}
        ]

def generate_direct_answer(user_id: str, message: str) -> str:
    """
    Generate a direct answer when no data sources are needed
    
    Args:
        user_id: Student ID
        message: The student's question
        
    Returns:
        Direct answer from the LLM
    """
    try:
        # Get API key from Secrets Manager
        api_key = get_api_key_from_secrets()
        
        # Create the agent using the teammate's code
        from agent.agent_logic import create_portal_agent
        agent = create_portal_agent()
        
        # Format the message with student ID context
        context_prompt = f"Student id: {user_id}. {message}"
        
        # Process the message through the agent
        response = agent.run(context_prompt)
        return response
        
    except Exception as e:
        print(f"Error generating direct answer: {str(e)}")
        return "I'm sorry, I encountered an error while processing your question. Please try again or contact student services for assistance."

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
