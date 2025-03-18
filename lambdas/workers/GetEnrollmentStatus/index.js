const AWS = require('aws-sdk');
const { Client } = require('pg');

const secretsManager = new AWS.SecretsManager();
const eventBridge = new AWS.EventBridge();

const CONFIG = {
    dbSecretArn: process.env.DB_SECRET_ARN,
    dbHost: process.env.DB_HOST,
    dbName: process.env.DB_NAME,
    dbPort: process.env.DB_PORT
};

exports.handler = async (event) => {
    console.log('Received event:', JSON.stringify(event));
    
    const { studentId, correlationId } = event.detail || {};
    
    if (!studentId) {
        await publishToEventBridge(correlationId, 'GetEnrollmentStatus', {
            status: 'ERROR',
            error: 'Missing required studentId parameter'
        });
        return;
    }
    
    try {
        const client = await connectToDatabase();
        
        // Get student's enrollment and program details
        const result = await client.query(
            `SELECT s.student_id, s.name, s.start_year, s.graduation_year,
                    e.enrollment_status, e.gpa, e.start_date,
                    p.program_id, p.program_name, p.director
             FROM students s
             JOIN enrollments e ON s.student_id = e.student_id
             JOIN programs p ON e.program_id = p.program_id
             WHERE s.student_id = $1
             AND e.enrollment_status = 'ACTIVE'
             LIMIT 1`,
            [studentId]
        );
        
        if (result.rows.length === 0) {
            await client.end();
            await publishToEventBridge(correlationId, 'GetEnrollmentStatus', {
                status: 'ERROR',
                error: 'No active enrollment found for student'
            });
            return;
        }
        
        const enrollment = result.rows[0];
        
        // Get current semester based on start year
        const currentYear = new Date().getFullYear();
        const yearsCompleted = currentYear - enrollment.start_year;
        const currentSemester = yearsCompleted * 2 + 1; // Assuming 2 semesters per year
        
        // Calculate time to graduation
        const graduationDate = new Date(enrollment.graduation_year, 5, 1); // June 1st
        const today = new Date();
        const monthsToGraduation = (graduationDate - today) / (1000 * 60 * 60 * 24 * 30);
        
        await client.end();
        
        // Publish success response to EventBridge
        await publishToEventBridge(correlationId, 'GetEnrollmentStatus', {
            status: 'SUCCESS',
            data: {
                studentId: enrollment.student_id,
                name: enrollment.name,
                enrollment: {
                    status: enrollment.enrollment_status,
                    startDate: enrollment.start_date,
                    gpa: enrollment.gpa
                },
                program: {
                    id: enrollment.program_id,
                    name: enrollment.program_name,
                    director: enrollment.director
                },
                academicProgress: {
                    startYear: enrollment.start_year,
                    graduationYear: enrollment.graduation_year,
                    currentSemester,
                    yearsCompleted
                },
                graduation: {
                    expectedDate: graduationDate.toISOString(),
                    monthsRemaining: Math.ceil(monthsToGraduation)
                }
            }
        });
        
    } catch (error) {
        console.error('Error getting enrollment status:', error);
        await publishToEventBridge(correlationId, 'GetEnrollmentStatus', {
            status: 'ERROR',
            error: `Error getting enrollment status: ${error.message}`
        });
    }
};

/**
 * Publish response to EventBridge
 */
async function publishToEventBridge(correlationId, workerName, data) {
    const params = {
        Entries: [{
            Source: 'student.query.worker',
            DetailType: `${workerName}Response`,
            Detail: JSON.stringify({
                correlationId,
                workerName,
                data,
                timestamp: new Date().toISOString()
            }),
            EventBusName: 'default',
            Time: new Date()
        }]
    };

    try {
        await eventBridge.putEvents(params).promise();
        console.log(`Published ${workerName} response to EventBridge for correlation ID: ${correlationId}`);
    } catch (error) {
        console.error('Error publishing to EventBridge:', error);
        throw error;
    }
}

async function connectToDatabase() {
    try {
        const secretResponse = await secretsManager.getSecretValue({
            SecretId: CONFIG.dbSecretArn
        }).promise();
        
        const password = secretResponse.SecretString;
        
        const client = new Client({
            host: CONFIG.dbHost,
            database: CONFIG.dbName,
            user: 'dbadmin',
            password: password,
            port: CONFIG.dbPort
        });
        
        await client.connect();
        return client;
    } catch (error) {
        console.error('Error connecting to database:', error);
        throw new Error(`Database connection error: ${error.message}`);
    }
} 