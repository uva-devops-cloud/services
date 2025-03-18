const { Client } = require('pg');
const AWS = require('aws-sdk');

// filepath: c:\Users\Merli\Dev\DevOps\services\lambda\worker\GetStudentData\index.js

// Initialize AWS clients
const secretsManager = new AWS.SecretsManager();
const eventBridge = new AWS.EventBridge();

/**
 * Lambda handler for GetStudentData 
 * Processes GetStudentData events from EventBridge
 */
exports.handler = async (event, context) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    let client = null;
    
    try {
        // Verify this is the correct event source and type
        if (event.source !== 'student.query.orchestrator' || 
            (event['detail-type'] !== 'GetStudentData' && !event.detail?.action === 'GetStudentData')) {
            console.log('Not a valid GetStudentData event');
            return {
                statusCode: 400,
                body: JSON.stringify({ error: 'Invalid event format or source' })
            };
        }
        
        // Extract studentId from event
        const { studentId, correlationId } = event.detail;
        
        if (!studentId) {
            throw new Error('Student ID is required');
        }
        
        // Get database credentials and create connection
        const dbConfig = await getDatabaseConfig();
        client = new Client(dbConfig);
        
        console.log('Connecting to database...');
        await client.connect();
        console.log('Connected successfully to database');
        
        // Query student details
        const studentDetails = await queryStudentDetails(client, studentId);
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                requestId: event.id,
                studentId: studentId,
                source: event.source,
                result: studentDetails
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
 * Query database for student details
 */
async function queryStudentDetails(client, studentId) {
    try {
        console.log(`Querying student details for student: ${studentId}`);
        
        // Main student information query
        const studentQuery = `
            SELECT 
                student_id,
                name,
                profile_photo,
                start_year,
                graduation_year,
                address
            FROM 
                students
            WHERE 
                student_id = $1
        `;
        
        const result = await client.query(studentQuery, [studentId]);
        
        if (result.rows.length === 0) {
            return { message: 'Student not found' };
        }
        
        return result.rows[0];
    } catch (error) {
        console.error('Database query error:', error);
        throw new Error(`Database query failed: ${error.message}`);
    }
}
