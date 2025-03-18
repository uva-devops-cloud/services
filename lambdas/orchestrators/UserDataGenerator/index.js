const AWS = require('aws-sdk');
const { Client } = require('pg');
const { v4: uuidv4 } = require('uuid');

/**
 * User Data Generator Lambda Function
 * 
 * This Lambda function generates random academic data for newly registered users.
 * It is triggered when a user makes their first query and no data exists for them yet.
 * 
 * Flow:
 * 1. Receive Cognito sub and username from Query Intake Lambda
 * 2. Check if user already exists in database
 * 3. If new user, generate random student data (courses, grades, etc.)
 * 4. Return the generated student ID
 */

// AWS service clients
const secretsManager = new AWS.SecretsManager();

// Configuration
const CONFIG = {
    dbSecretArn: process.env.DB_SECRET_ARN || 'arn:aws:secretsmanager:region:account-id:secret:db-creds',
    dbHost: process.env.DB_HOST || 'localhost',
    dbName: process.env.DB_NAME || 'studentportal',
    dbPort: process.env.DB_PORT || 5432
};

/**
 * Main Lambda handler function
 * 
 * @param {Object} event - Event from Query Intake Lambda
 * @param {Object} context - Lambda context
 * @returns {Object} - Response with generated student ID
 */
exports.handler = async (event, context) => {
    console.log('Received request:', JSON.stringify(event));
    
    const { cognitoSub, username } = event;
    
    if (!cognitoSub) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                message: 'Missing required Cognito sub parameter',
                status: 'error'
            })
        };
    }
    
    try {
        // Check if user already exists
        const userExists = await checkIfUserExists(cognitoSub);
        
        if (userExists) {
            console.log(`User with cognito sub ${cognitoSub} already exists in database`);
            return {
                statusCode: 200,
                body: JSON.stringify({
                    message: 'User data already exists',
                    studentId: userExists.studentId,
                    status: 'exists'
                })
            };
        }
        
        // Generate random student data
        const result = await generateRandomUserData(cognitoSub, username || 'New Student');
        
        return {
            statusCode: 201,
            body: JSON.stringify({
                message: 'User data generated successfully',
                studentId: result.studentId,
                status: 'created'
            })
        };
    } catch (error) {
        console.error('Error in UserDataGenerator Lambda:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: `An error occurred: ${error.message}`,
                status: 'error'
            })
        };
    }
};

/**
 * Connect to the PostgreSQL database
 * 
 * @returns {Client} - Database client
 */
async function connectToDatabase() {
    try {
        // Get database password from Secrets Manager
        const secretResponse = await secretsManager.getSecretValue({
            SecretId: CONFIG.dbSecretArn
        }).promise();
        
        const password = secretResponse.SecretString;
        
        // Create database client
        const client = new Client({
            host: CONFIG.dbHost,
            database: CONFIG.dbName,
            user: 'dbadmin',
            password: password,
            port: CONFIG.dbPort,
            connectionTimeoutMillis: 10000,
            query_timeout: 30000
        });
        
        // Connect to database
        await client.connect();
        
        return client;
    } catch (error) {
        console.error('Error connecting to database:', error);
        throw new Error(`Database connection error: ${error.message}`);
    }
}

/**
 * Check if user already exists in database
 * 
 * @param {string} cognitoSub - Cognito sub identifier
 * @returns {Object|false} - Student ID if exists, false otherwise
 */
async function checkIfUserExists(cognitoSub) {
    const client = await connectToDatabase();
    
    try {
        const result = await client.query(
            'SELECT student_id FROM students WHERE cognito_info = $1',
            [cognitoSub]
        );
        
        if (result.rows.length > 0) {
            return { studentId: result.rows[0].student_id };
        }
        
        return false;
    } catch (error) {
        console.error('Error checking if user exists:', error);
        throw error;
    } finally {
        await client.end();
    }
}

/**
 * Generate random academic data for a new user
 * 
 * @param {string} cognitoSub - Cognito sub identifier
 * @param {string} username - User's display name
 * @returns {Object} - Generated student ID
 */
async function generateRandomUserData(cognitoSub, username) {
    const client = await connectToDatabase();
    
    try {
        // Start transaction
        await client.query('BEGIN');
        
        // Get current year for realistic data
        const currentYear = new Date().getFullYear();
        
        // 1. Insert into students table, storing the Cognito sub in cognito_info
        const studentResult = await client.query(
            `INSERT INTO students (name, cognito_info, start_year, graduation_year) 
             VALUES ($1, $2, $3, $4) 
             RETURNING student_id`,
            [username, cognitoSub, currentYear - 1, currentYear + 3]
        );
        
        const studentId = studentResult.rows[0].student_id;
        console.log(`Created new student with ID: ${studentId}`);
        
        // 2. Get random program (or create one if none exist)
        let programResult = await client.query(
            'SELECT program_id, program_name FROM programs ORDER BY RANDOM() LIMIT 1'
        );
        
        let programId, programName;
        
        if (programResult.rows.length === 0) {
            // No programs exist, create a default one
            const defaultPrograms = [
                { name: 'Computer Science', director: 'Dr. Alan Turing' },
                { name: 'Business Administration', director: 'Dr. Peter Drucker' },
                { name: 'Psychology', director: 'Dr. Carl Jung' }
            ];
            
            const randomProgram = defaultPrograms[Math.floor(Math.random() * defaultPrograms.length)];
            
            const newProgramResult = await client.query(
                `INSERT INTO programs (program_name, director) 
                 VALUES ($1, $2) 
                 RETURNING program_id, program_name`,
                [randomProgram.name, randomProgram.director]
            );
            
            programId = newProgramResult.rows[0].program_id;
            programName = newProgramResult.rows[0].program_name;
            console.log(`Created new program with ID: ${programId}`);
        } else {
            programId = programResult.rows[0].program_id;
            programName = programResult.rows[0].program_name;
        }
        
        // 3. Enroll student in program
        await client.query(
            `INSERT INTO enrollments (student_id, program_id, gpa, enrollment_status, start_date) 
             VALUES ($1, $2, $3, $4, $5)`,
            [studentId, programId, (Math.random() * 4).toFixed(2), 'ACTIVE', new Date()]
        );
        
        console.log(`Enrolled student ${studentId} in program ${programId}`);
        
        // 4. Get courses for the program (or create some if none exist)
        let coursesResult = await client.query(
            `SELECT c.course_id 
             FROM courses c 
             JOIN program_courses pc ON c.course_id = pc.course_id 
             WHERE pc.program_id = $1 
             LIMIT 5`,
            [programId]
        );
        
        if (coursesResult.rows.length === 0) {
            // No courses exist for this program, create default ones
            const defaultCourses = [
                { code: `${programName.substring(0, 3).toUpperCase()}101`, name: 'Introduction to ' + programName, credits: 3 },
                { code: `${programName.substring(0, 3).toUpperCase()}201`, name: 'Intermediate ' + programName, credits: 4 },
                { code: `${programName.substring(0, 3).toUpperCase()}301`, name: 'Advanced ' + programName, credits: 4 },
                { code: `${programName.substring(0, 3).toUpperCase()}401`, name: programName + ' Capstone', credits: 5 },
                { code: 'CORE100', name: 'Critical Thinking', credits: 3 }
            ];
            
            for (const course of defaultCourses) {
                // Create course
                const newCourseResult = await client.query(
                    `INSERT INTO courses (course_code, course_name, credits, semester) 
                     VALUES ($1, $2, $3, $4) 
                     RETURNING course_id`,
                    [course.code, course.name, course.credits, 'Fall ' + currentYear]
                );
                
                const courseId = newCourseResult.rows[0].course_id;
                
                // Link course to program
                await client.query(
                    `INSERT INTO program_courses (program_id, course_id) 
                     VALUES ($1, $2)`,
                    [programId, courseId]
                );
                
                // Enroll student in course with random grade
                const gradeOptions = ['A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D', 'F'];
                const randomGrade = gradeOptions[Math.floor(Math.random() * gradeOptions.length)];
                const status = Math.random() > 0.2 ? 'COMPLETED' : 'IN_PROGRESS';
                
                await client.query(
                    `INSERT INTO student_course_enrollment 
                     (student_id, course_id, program_id, grade, status, semester) 
                     VALUES ($1, $2, $3, $4, $5, $6)`,
                    [studentId, courseId, programId, randomGrade, status, 'Fall ' + currentYear]
                );
                
                console.log(`Enrolled student ${studentId} in course ${courseId} with grade ${randomGrade}`);
            }
        } else {
            // Enroll student in existing courses
            for (const course of coursesResult.rows) {
                const gradeOptions = ['A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D', 'F'];
                const randomGrade = gradeOptions[Math.floor(Math.random() * gradeOptions.length)];
                const status = Math.random() > 0.2 ? 'COMPLETED' : 'IN_PROGRESS';
                
                await client.query(
                    `INSERT INTO student_course_enrollment 
                     (student_id, course_id, program_id, grade, status, semester) 
                     VALUES ($1, $2, $3, $4, $5, $6)`,
                    [studentId, course.course_id, programId, randomGrade, status, 'Fall ' + currentYear]
                );
                
                console.log(`Enrolled student ${studentId} in course ${course.course_id} with grade ${randomGrade}`);
            }
        }
        
        // 5. Set up usage info
        await client.query(
            `INSERT INTO usage_info (student_id, credits_available, credits_used) 
             VALUES ($1, $2, $3)`,
            [studentId, 100, 0]
        );
        
        console.log(`Set up usage info for student ${studentId}`);
        
        // Commit transaction
        await client.query('COMMIT');
        
        return { studentId };
        
    } catch (error) {
        // Rollback transaction on error
        await client.query('ROLLBACK');
        console.error('Error generating user data:', error);
        throw error;
    } finally {
        await client.end();
    }
}

/**
 * Helper function to get random number in range
 * 
 * @param {number} min - Minimum value
 * @param {number} max - Maximum value
 * @returns {number} - Random number in range
 */
function getRandomNumber(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
