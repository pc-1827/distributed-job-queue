const cronParser = require('cron-parser');
const Job = require('../job');

async function scheduleRepeatedJob(redisClient, queueName, job, jobOptions, generateJobId, jobDataUpdater) {
    try {
        const interval = cronParser.parseExpression(jobOptions.repeat.cron);
        const nextRun = interval.next().getTime();
        const delay = nextRun - Date.now();

        const newJob = new Job(job, jobOptions);
        const newJobId = await generateJobId();
        await jobDataUpdater(redisClient, queueName, newJob, newJobId);

        await redisClient.lPush(`jobQueue:${queueName}:delayed`, newJobId.toString());
        await redisClient.publish("delayed", "change");

        setTimeout(async () => {
            await redisClient.lRem(`jobQueue:${queueName}:delayed`, 0, newJobId.toString());
            await redisClient.publish("delayed", "change");

            const priority = jobOptions.priority || 999;
            await redisClient.zAdd(`jobQueue:${queueName}:waiting`, [{ score: priority, value: newJobId.toString() }]);
            await redisClient.publish("waiting", "change");

            console.log('Repeated job added to the queue:', newJobId);

            // Schedule the next run
            scheduleRepeatedJob(redisClient, queueName, job, jobOptions, generateJobId, jobDataUpdater);
        }, delay);
    } catch (error) {
        console.error('Error scheduling repeated job:', error);
    }
}

module.exports = scheduleRepeatedJob;
