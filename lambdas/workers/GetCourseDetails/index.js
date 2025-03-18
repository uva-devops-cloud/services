const { Client } = require('pg');
const AWS = require('aws-sdk');

// Initialize AWS services
const secretsManager = new AWS.SecretsManager();

// Helper for logging with timestamps
const logWithTimestamp = (message) => {
  console.log(`[${new Date().toISOString()}] ${message}`);
};

// Get database credentials from Secrets Manager
async function getDatabaseCredentials() {
  try {
    const secretData = await secretsManager.getSecretValue({ 
      SecretId: process.env.DB_SECRET_ARN 
    }).promise();
    
    let credentials;
    try {
      credentials = JSON.parse(secretData.SecretString);
    } catch (e) {
      // If not JSON, assume it's just the password
      credentials = { 
        password: secretData.SecretString,
        user: 'dbadmin',
        host: process.env.DB_HOST,
        database: process.env.DB_NAME,
        port: 5432
      };
    }
    
    return credentials;
  } catch (error) {
    logWithTimestamp(`Failed to retrieve database credentials: ${error.message}`);
    throw error;
  }
}

// Query course details from database
async function getCourseDetails(courseIdentifier, identifierType = 'id') {
  let client;
  try {
    // Get database credentials
    const credentials = await getDatabaseCredentials();
    
    // Connect to the database
    client = new Client({
      user: credentials.user || 'dbadmin',
      password: credentials.password,
      host: credentials.host || process.env.DB_HOST,
      database: credentials.database || process.env.DB_NAME,
      port: credentials.port || 5432,
      connectionTimeoutMillis: 10000
    });
    
    await client.connect();
    logWithTimestamp('Connected to the database');
    
    // Determine query based on identifier type
    let query;
    let params;
    
    if (identifierType === 'code') {
      query = `
        SELECT c.*, 
               array_agg(DISTINCT p.program_name) as programs,
               COUNT(DISTINCT sce.student_id) as enrolled_students
        FROM courses c
        LEFT JOIN program_courses pc ON c.course_id = pc.course_id
        LEFT JOIN programs p ON pc.program_id = p.program_id
        LEFT JOIN student_course_enrollment sce ON c.course_id = sce.course_id
        WHERE c.course_code = $1
        GROUP BY c.course_id
      `;
      params = [courseIdentifier];
    } else {
      query = `
        SELECT c.*,
               array_agg(DISTINCT p.program_name) as programs,
               COUNT(DISTINCT sce.student_id) as enrolled_students
        FROM courses c
        LEFT JOIN program_courses pc ON c.course_id = pc.course_id
        LEFT JOIN programs p ON pc.program_id = p.program_id
        LEFT JOIN student_course_enrollment sce ON c.course_id = sce.course_id
        WHERE c.course_id = $1
        GROUP BY c.course_id
      `;
      params = [parseInt(courseIdentifier)];
    }
    
    // Execute query
    const result = await client.query(query, params);
    
    if (result.rows.length === 0) {
      return { found: false, message: 'Course not found' };
    }
    
    return { 
      found: true,
      course: result.rows[0]
    };
    
  } catch (error) {
    logWithTimestamp(`Error retrieving course details: ${error.message}`);
    throw error;
  } finally {
    if (client) {
      await client.end();
      logWithTimestamp('Database connection closed');
    }
  }
}

// Lambda handler
exports.handler = async (event) => {
  try {
    logWithTimestamp('Received event: ' + JSON.stringify(event));
    
    // Check if this is an EventBridge event
    if (event.source === 'student.query.orchestrator' && 
        event.detail && 
        (event.detail.action === 'GetCourseDetails' || event['detail-type'] === 'GetCourseDetails')) {
      
      const { courseId, courseCode } = event.detail;
      
      if (!courseId && !courseCode) {
        return {
          statusCode: 400,
          body: JSON.stringify({ 
            error: 'Either courseId or courseCode must be provided' 
          })
        };
      }
      
      // Use course code if provided, otherwise use ID
      const identifier = courseCode || courseId;
      const identifierType = courseCode ? 'code' : 'id';
      
      // Get course details
      const courseDetails = await getCourseDetails(identifier, identifierType);
      
      return {
        statusCode: courseDetails.found ? 200 : 404,
        body: JSON.stringify(courseDetails)
      };
    }
    
    return {
      statusCode: 400,
      body: JSON.stringify({ error: 'Invalid event format' })
    };
    
  } catch (error) {
    logWithTimestamp(`Error processing request: ${error.message}`);
    
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Internal server error' })
    };
  }
};