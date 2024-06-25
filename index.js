const redis = require('redis');
const dotenv = require('dotenv');

dotenv.config();
const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = process.env;

const redisClient = redis.createClient({
    url: `redis://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}`
});

redisClient.on('error', (err) => console.log('Redis Client Error', err));

(async () => {
    await redisClient.connect();

    // Queue name
    const queueName = 'jobQueue';

    // Function to add a job to the queue
    async function addJob(job) {
        await redisClient.lPush(queueName, JSON.stringify(job));
        console.log('Job added to the queue:', job);
    }

    // Function to process jobs from the queue
    async function processJobs() {
        while (true) {
            const job = await redisClient.rPop(queueName);
            if (job) {
                const parsedJob = JSON.parse(job);
                console.log('Processing job:', parsedJob);

                // Simulate job processing
                await handleJob(parsedJob);
            } else {
                console.log('No jobs in the queue, waiting...');
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    // Example job handler
    async function handleJob(job) {
        // Simulate job processing time
        await new Promise(resolve => setTimeout(resolve, 10000));
        console.log('Job processed:', job);
    }

    for(let i = 0; i < 5; i++) {
            // Adding example jobs to the queue
    addJob({ id: 1, type: 'sendEmail', payload: { to: 'user1@example.com' } });
    addJob({ id: 2, type: 'generateReport', payload: { reportId: 'report1' } });
    }

    // Start processing jobs
    processJobs();
})();
