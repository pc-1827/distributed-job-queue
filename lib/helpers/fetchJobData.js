const Job = require('../job');

async function fetchJobData(redisClient, queueName, jobId) {
    const jobData = await redisClient.hGetAll(`jobQueue:${queueName}:${jobId}`);
    if (Object.keys(jobData).length === 0) {
        return null;
    }

    const parsedJobData = {};
    for (const [key, value] of Object.entries(jobData)) {
        try {
            parsedJobData[key] = JSON.parse(value);
        } catch (error) {
            parsedJobData[key] = value;
        }
    }

    const job = parsedJobData.data ? parsedJobData.data : {};
    const jobOptions = parsedJobData.jobOptions ? parsedJobData.jobOptions : {};

    return new Job(job, jobOptions);
}

module.exports = fetchJobData;
