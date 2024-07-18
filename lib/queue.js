const redis = require('redis');
const Job = require('./job');
const { Semaphore } = require('async-mutex');

class Queue {
    constructor(name, queueOptions) {
        this.name = name;
        this.queueOptions = queueOptions;
        this.jobsMap = new Map();

        this.redisOptions = this.queueOptions.redis;
        this.redisClient = null;
        this.ready = this.init();
    }

    async init() {
        try {
            const redisUrl = `redis://${this.redisOptions.password ? ':' + this.redisOptions.password + '@' : ''}${this.redisOptions.host}:${this.redisOptions.port}`;
            this.redisClient = redis.createClient({ url: redisUrl });
            await this.redisClient.connect();
            console.log('Connected to Redis with password.');
        } catch (error) {
            console.error('Failed to connect to Redis with password, trying without password...');
            try {
                const redisUrl = `redis://${this.redisOptions.host}:${this.redisOptions.port}`;
                this.redisClient = redis.createClient({ url: redisUrl });
                await this.redisClient.connect();
                console.log('Connected to Redis without password.');
            } catch (error) {
                throw new Error('Failed to connect to Redis without password.');
            }
        }
    }

    async addJobs(job, jobOptions) {
        await this.ready;
        try {
            const newJob = new Job(job, jobOptions);
            const jobId = await this.generateJobId();
            this.jobsMap.set(jobId, newJob);
            await this.jobDataUpdater(newJob, jobId);

            const priority = jobOptions.priority || 999;
            await this.redisClient.zAdd(`jobQueue:${this.name}:waiting`, [{ score: priority, value: jobId.toString() }]);

            console.log('Job added to the queue:', jobId);
        } catch (error) {
            console.error('Error adding job to the queue:', error);
        }
    }

    async generateJobId() {
        const jobId = await this.redisClient.incr('jobIdCounter');
        return jobId;
    }

    async processJobs(callback, concurrency) {
        await this.ready;
        const semaphore = new Semaphore(concurrency);

        const processJob = async (jobId) => {
            const jobInstance = this.jobsMap.get(jobId);

            if (!jobInstance) {
                console.error(`No job instance found for job ID ${jobId}`);
                return;
            }

            await jobInstance.isProcessing();
            await this.jobDataUpdater(jobInstance, jobId);

            try {
                await new Promise((resolve) => {
                    const done = async (err) => {
                        if (err) {
                            if (jobInstance.jobOptions.attempts && jobInstance.jobOptions.attempts > 1) {
                                jobInstance.jobOptions.attempts--;
                                const priority = jobInstance.jobOptions.priority || 999;
                                await this.redisClient.zAdd(`jobQueue:${this.name}:waiting`, [{ score: priority, value: jobId.toString() }]);
                                console.log('Job failed, trying again:', jobId);
                            } else {
                                await jobInstance.isFailed();
                                await this.jobDataUpdater(jobInstance, jobId);
                                await this.jobFailedArrayUpdater(jobId);
                                console.log('Job failed:', jobId);
                            }
                        } else {
                            await jobInstance.isCompleted();
                            await this.jobDataUpdater(jobInstance, jobId);
                            await this.jobCompletedArrayUpdater(jobId);
                            console.log('Job completed:', jobId);
                        }
                        resolve();
                    };
                    callback(jobId, done);
                });

                await this.handleJob(jobId);
            } catch (error) {
                console.error('Error in job processing:', error);
                await jobInstance.isFailed();
                await this.jobDataUpdater(jobInstance, jobId);
                await this.jobFailedArrayUpdater(jobId);
            }
        };

        const worker = async () => {
            while (true) {
                try {
                    const job = await this.redisClient.eval(
                        `
                        local job = redis.call('zrange', KEYS[1], 0, 0)
                        if next(job) ~= nil then
                            redis.call('zrem', KEYS[1], job[1])
                            redis.call('lpush', KEYS[2], job[1])
                            return job[1]
                        end
                        return nil
                        `,
                        {
                            keys: [`jobQueue:${this.name}:waiting`, `jobQueue:${this.name}:active`],
                            arguments: []
                        }
                    );

                    if (job) {
                        const jobId = parseInt(job, 10);
                        await semaphore.acquire();
                        processJob(jobId).finally(() => semaphore.release());
                    } else {
                        //console.log('No jobs in the queue, waiting...');
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                } catch (error) {
                    console.error('Error fetching job from the queue:', error);
                }
            }
        };

        // Start workers
        for (let i = 0; i < concurrency; i++) {
            worker();
        }
    }

    async handleJob(jobId) {
        try {
            // Simulate job processing time
            await new Promise(resolve => setTimeout(resolve, 1000));
            //console.log('Job processed:', jobId);
        } catch (error) {
            console.error('Error handling job:', error);
        }
    }

    async jobDataUpdater(job, jobId) {
        try {
            for (let key in job) {
                await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, JSON.stringify(key), JSON.stringify(job[key]));
            }
        } catch (error) {
            console.error('Error updating job data:', error);
        }
    }

    async jobCompletedArrayUpdater(jobId) {
        try {
            await this.redisClient.lPush(`jobQueue:${this.name}:completed`, jobId.toString());
        } catch (error) {
            console.error('Error updating completed jobs array:', error);
        }
    }

    async jobFailedArrayUpdater(jobId) {
        try {
            await this.redisClient.lPush(`jobQueue:${this.name}:failed`, jobId.toString());
        } catch (error) {
            console.error('Error updating failed jobs array:', error);
        }
    }
}

module.exports = Queue;
