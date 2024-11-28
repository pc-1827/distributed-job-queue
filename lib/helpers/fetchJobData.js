const Job = require('../job');

async function fetchJobData(redisClient, queueName, jobId) {
    const jobKey = `jobQueue:${queueName}:${jobId}`;
    const jobDataString = await redisClient.hGet(jobKey, 'data');
    if (!jobDataString) {
        return null;
    }
    let jobObject;
    try {
        jobObject = JSON.parse(jobDataString);
    } catch (error) {
        console.error('Error parsing job data:', error);
        return null;
    }

    const job = new Job(jobObject.data, jobObject.jobOptions);
    job.status = jobObject.status;
    job.createdOn = new Date(jobObject.createdOn);
    job.processedOn = jobObject.processedOn ? new Date(jobObject.processedOn) : null;
    job.completedOn = jobObject.completedOn ? new Date(jobObject.completedOn) : null;

    return job;
}

module.exports = fetchJobData;
