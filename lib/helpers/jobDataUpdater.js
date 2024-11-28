async function jobDataUpdater(redisClient, queueName, job, jobId) {
    try {
        const jobKey = `jobQueue:${queueName}:${jobId}`;
        const jobData = JSON.stringify({
            data: job.data,
            jobOptions: job.jobOptions,
            status: job.status,
            createdOn: job.createdOn,
            processedOn: job.processedOn,
            completedOn: job.completedOn,
        });
        await redisClient.hSet(jobKey, 'data', jobData);
    } catch (error) {
        console.error('Error updating job data:', error);
    }
}

module.exports = jobDataUpdater;
