const redis = require('redis');
const Job = require('./job');
const TokenBucket = require('./token');
const { Semaphore } = require('async-mutex');
const fs = require('fs');
const path = require('path');
const scheduleRepeatedJob = require('./helpers/scheduleRepeatedJob');
const fetchJobData = require('./helpers/fetchJobData');
const jobDataUpdater = require('./helpers/jobDataUpdater');

class Queue {
    constructor(name, queueOptions) {
        this.name = name;
        this.queueOptions = queueOptions;

        this.redisOptions = this.queueOptions.redis;
        this.redisClient = null;
        this.ready = this.init();

        this.luaScripts = {
            fetchJob: fs.readFileSync(path.join(__dirname, 'commands/fetch_job.lua'), 'utf8'),
            completeJob: fs.readFileSync(path.join(__dirname, 'commands/complete_job.lua'), 'utf8'),
            retryJob: fs.readFileSync(path.join(__dirname, 'commands/retry_job.lua'), 'utf8'),
            failJob: fs.readFileSync(path.join(__dirname, 'commands/fail_job.lua'), 'utf8')
        };

        if (queueOptions.limit) {
            this.tokenBucket = new TokenBucket(queueOptions.limit.max, queueOptions.limit.duration);
        }
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
            await jobDataUpdater(this.redisClient, this.name, newJob, jobId);

            const priority = jobOptions.priority || 999;
            await this.redisClient.zAdd(`jobQueue:${this.name}:waiting`, [{ score: priority, value: jobId.toString() }]);
            await this.redisClient.publish("waiting", "change");

            console.log('Job added to the queue:', jobId);

            if (jobOptions.repeat && jobOptions.repeat.cron) {
                scheduleRepeatedJob(this.redisClient, this.name, job, jobOptions, this.generateJobId.bind(this), jobDataUpdater);
            }
        } catch (error) {
            console.error('Error adding job to the queue:', error);
        }
    }

    async generateJobId() {
        const jobId = await this.redisClient.incr('jobIdCounter');
        return jobId.toString();
    }

    async processJobs(callback, concurrency) {
        await this.ready;
        const semaphore = new Semaphore(concurrency);

        const processJob = async (jobId) => {
            const jobInstance = await fetchJobData(this.redisClient, this.name, jobId);

            if (!jobInstance) {
                console.error(`No job instance found for job ID ${jobId}`);
                return;
            }

            await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, 'status', 'processing');
            await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, 'processedOn', JSON.stringify(new Date()));
            await this.redisClient.publish("waiting", "change");

            try {
                await new Promise((resolve) => {
                    const done = async (err) => {
                        if (err) {
                            if (jobInstance.jobOptions.attempts && jobInstance.jobOptions.attempts > 1) {
                                await this.retryJob(jobId, jobInstance);
                                console.log('Job failed, trying again:', jobId);
                            } else {
                                await this.failJob(jobId);
                                console.log('Job failed:', jobId);
                            }
                        } else {
                            await this.completeJob(jobId);
                            console.log('Job completed:', jobId);
                        }
                        resolve();
                    };
                    callback(jobId, done);
                });

            } catch (error) {
                console.error('Error in job processing:', error);
                await this.failJob(jobId, jobInstance);
            }
        };

        const worker = async () => {
            while (true) {
                try {
                    if (!this.tokenBucket || this.tokenBucket.consume()) {
                        const job = await this.redisClient.eval(this.luaScripts.fetchJob, {
                            keys: [
                                `jobQueue:${this.name}:waiting`,
                                `jobQueue:${this.name}:active`
                            ],
                            arguments: []
                        });

                        if (job) {
                            const jobId = job.toString();
                            await semaphore.acquire();
                            processJob(jobId).finally(() => semaphore.release());
                        } else {
                            await new Promise(resolve => setTimeout(resolve, 1000));
                        }
                    } else {
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                } catch (error) {
                    console.error('Error fetching job from the queue:', error);
                }
            }
        };

        for (let i = 0; i < concurrency; i++) {
            worker();
        }
    }

    async completeJob(jobId) {
        await this.redisClient.eval(this.luaScripts.completeJob, {
            keys: [
                `jobQueue:${this.name}:active`,
                `jobQueue:${this.name}:completed`,
                `jobQueue:${this.name}:${jobId}`
            ],
            arguments: [jobId.toString(), JSON.stringify(new Date())]
        });
    }

    async failJob(jobId) {
        await this.redisClient.eval(this.luaScripts.failJob, {
            keys: [
                `jobQueue:${this.name}:active`,
                `jobQueue:${this.name}:failed`,
                `jobQueue:${this.name}:${jobId}`
            ],
            arguments: [jobId.toString()]
        });
    }

    async retryJob(jobId, jobInstance) {
        await this.redisClient.eval(this.luaScripts.retryJob, {
            keys: [
                `jobQueue:${this.name}:active`,
                `jobQueue:${this.name}:failed`,
                `jobQueue:${this.name}:waiting`,
                `jobQueue:${this.name}:${jobId}`
            ],
            arguments: [jobId.toString()]
        });
        jobInstance.jobOptions.attempts--;
        await jobDataUpdater(this.redisClient, this.name, jobInstance, jobId);
    }
}

module.exports = Queue;
