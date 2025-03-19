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
    
    const { studentId, incrementUsage = false, correlationId } = event.detail || {};
    
    if (!studentId) {
        await publishToEventBridge(correlationId, 'GetUsageInfo', {
            status: 'ERROR',
            error: 'Missing required studentId parameter'
        });
        return;
    }
    
    let client = null;
    try {
        client = await connectToDatabase();
        
        // Start transaction
        await client.query('BEGIN');
        
        // Get current usage info
        const result = await client.query(
            `SELECT credits_available, credits_used
             FROM usage_info
             WHERE student_id = $1`,
            [studentId]
        );
        
        if (result.rows.length === 0) {
            // Create new usage record if none exists
            await client.query(
                `INSERT INTO usage_info (student_id, credits_available, credits_used)
                 VALUES ($1, 100, 0)`,
                [studentId]
            );
            
            await client.query('COMMIT');
            await client.end();
            
            await publishToEventBridge(correlationId, 'GetUsageInfo', {
                status: 'SUCCESS',
                data: {
                    studentId,
                    creditsAvailable: 100,
                    creditsUsed: 0
                }
            });
            return;
        }
        
        const usageInfo = result.rows[0];
        
        if (incrementUsage) {
            // Update usage if credits are available
            if (usageInfo.credits_available > 0) {
                await client.query(
                    `UPDATE usage_info
                     SET credits_available = credits_available - 1,
                         credits_used = credits_used + 1
                     WHERE student_id = $1`,
                    [studentId]
                );
                
                usageInfo.credits_available--;
                usageInfo.credits_used++;
            }
        }
        
        await client.query('COMMIT');
        await client.end();
        
        await publishToEventBridge(correlationId, 'GetUsageInfo', {
            status: 'SUCCESS',
            data: {
                studentId,
                creditsAvailable: usageInfo.credits_available,
                creditsUsed: usageInfo.credits_used
            }
        });
        
    } catch (error) {
        // Rollback transaction on error
        if (client) {
            await client.query('ROLLBACK');
            await client.end();
        }
        
        console.error('Error getting usage info:', error);
        await publishToEventBridge(correlationId, 'GetUsageInfo', {
            status: 'ERROR',
            error: `Error getting usage info: ${error.message}`
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
            EventBusName: 'main-event-bus',
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