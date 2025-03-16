const { Client } = require('pg');
const AWS = require('aws-sdk');

// Initialize AWS clients
const secretsManager = new AWS.SecretsManager();

/**
 * Lambda handler for ProgramDetails 
 * Processes GetProgramDetails events from EventBridge
 */
exports.handler = async (event, context) => {
  console.log('Received event:', JSON.stringify(event, null, 2));
  let client = null;
  
  try {
    // Extract studentId from event
    const { studentId } = event.detail;
    
    if (!studentId) {
      throw new Error('Student ID is required');
    }
    
    // Get database credentials and create connection
    const dbConfig = await getDatabaseConfig();
    client = new Client(dbConfig);
    
    console.log('Connecting to database...');
    await client.connect();
    console.log('Connected successfully to database');
    
    // Query program details for the student
    const programDetails = await queryProgramDetails(client, studentId);
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        requestId: event.id,
        studentId: studentId,
        source: event.source,
        result: programDetails
      })
    };
  } catch (error) {
    console.error('Error:', error.message);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  } finally {
    if (client) {
      try {
        await client.end();
        console.log('Database connection closed');
      } catch (err) {
        console.error('Error closing database connection:', err);
      }
    }
  }
};

/**
 * Get database configuration using secrets manager
 */
async function getDatabaseConfig() {
  try {
    // Get database password from Secrets Manager
    const { SecretString } = await secretsManager.getSecretValue({
      SecretId: process.env.DB_SECRET_ARN
    }).promise();
    
    console.log('Retrieved database credentials');
    
    return {
      host: process.env.DB_HOST,
      database: process.env.DB_NAME,
      user: 'dbadmin',
      password: SecretString,
      port: 5432,
      connectionTimeoutMillis: 10000,
      query_timeout: 30000
    };
  } catch (error) {
    console.error('Error retrieving database credentials:', error);
    throw new Error(`Failed to get database credentials: ${error.message}`);
  }
}

/**
 * Query database for program details
 */
async function queryProgramDetails(client, studentId) {
  try {
    console.log(`Querying program details for student: ${studentId}`);
    
    // Query that gets program information for a student based on the actual schema
    const query = `
      SELECT 
        p.program_id,
        p.program_name,
        p.director,
        e.gpa,
        e.enrollment_status,
        e.start_date
      FROM 
        enrollments e
      JOIN 
        programs p ON e.program_id = p.program_id
      WHERE 
        e.student_id = $1
      ORDER BY 
        e.start_date DESC
      LIMIT 1
    `;
    
    const result = await client.query(query, [studentId]);
    
    if (result.rows.length === 0) {
      return { message: 'No program details found for this student' };
    }
    
    return result.rows[0];
  } catch (error) {
    console.error('Database query error:', error);
    throw new Error(`Database query failed: ${error.message}`);
  }
}