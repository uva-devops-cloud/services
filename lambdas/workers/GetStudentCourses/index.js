const { Client } = require('pg');
const AWS = require('aws-sdk');

// filepath: c:\Users\Merli\Dev\DevOps\services\lambda\worker\GetStudentCourses\index.js

// Initialize AWS clients
const secretsManager = new AWS.SecretsManager();

/**
 * Lambda handler for GetStudentCourses 
 * Processes GetStudentCourses events from EventBridge
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
        
        // Query student courses
        const studentCourses = await queryStudentCourses(client, studentId);
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                requestId: event.id,
                studentId: studentId,
                source: event.source,
                result: studentCourses
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
 * Query database for student's enrolled courses
 */
async function queryStudentCourses(client, studentId) {
    try {
        console.log(`Querying courses for student: ${studentId}`);
        
        // Query joining student_course_enrollment with courses
        const coursesQuery = `
            SELECT 
                c.course_id,
                c.course_code,
                c.course_name,
                c.credits,
                c.semester,
                sce.grade,
                sce.status,
                sce.semester as enrollment_semester,
                p.program_name
            FROM 
                student_course_enrollment sce
            JOIN 
                courses c ON sce.course_id = c.course_id
            LEFT JOIN
                programs p ON sce.program_id = p.program_id
            WHERE 
                sce.student_id = $1
        `;
        
        const result = await client.query(coursesQuery, [studentId]);
        
        if (result.rows.length === 0) {
            return { message: 'No courses found for this student' };
        }
        
        return result.rows;
    } catch (error) {
        console.error('Database query error:', error);
        throw new Error(`Database query failed: ${error.message}`);
    }
}