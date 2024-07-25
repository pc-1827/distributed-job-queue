async function jobDataUpdater(redisClient, queueName, job, jobId) {
    try {
        for (let key in job) {
            await redisClient.hSet(`jobQueue:${queueName}:${jobId}`, key.toString(), JSON.stringify(job[key]));
        }
    } catch (error) {
        console.error('Error updating job data:', error);
    }
}

module.exports = jobDataUpdater;
