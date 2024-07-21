const redis = require('redis');
const Job = require('./job');
const TokenBucket = require('./token');
const { Semaphore } = require('async-mutex');
const fs = require('fs');
const path = require('path');
const cronParser = require('cron-parser');

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

        // Initialize token bucket only if limit is defined
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
            await this.jobDataUpdater(newJob, jobId);

            const priority = jobOptions.priority || 999;
            await this.redisClient.zAdd(`jobQueue:${this.name}:waiting`, [{ score: priority, value: jobId.toString() }]);

            console.log('Job added to the queue:', jobId);

            if (jobOptions.repeat && jobOptions.repeat.cron) {
                this.scheduleRepeatedJob(job, jobOptions);
            }
        } catch (error) {
            console.error('Error adding job to the queue:', error);
        }
    }

    async scheduleRepeatedJob(job, jobOptions) {
        try {
            const interval = cronParser.parseExpression(jobOptions.repeat.cron);
            const nextRun = interval.next().getTime();
            const delay = nextRun - Date.now();

            const newJob = new Job(job, jobOptions);
            const newJobId = await this.generateJobId();
            await this.jobDataUpdater(newJob, newJobId);

            await this.redisClient.lPush(`jobQueue:${this.name}:delayed`, newJobId.toString());

            setTimeout(async () => {
                await this.redisClient.lRem(`jobQueue:${this.name}:delayed`, 0, newJobId.toString());

                const priority = jobOptions.priority || 999;
                await this.redisClient.zAdd(`jobQueue:${this.name}:waiting`, [{ score: priority, value: newJobId.toString() }]);

                console.log('Repeated job added to the queue:', newJobId);

                // Schedule the next run
                this.scheduleRepeatedJob(job, jobOptions);
            }, delay);
        } catch (error) {
            console.error('Error scheduling repeated job:', error);
        }
    }

    async generateJobId() {
        const jobId = await this.redisClient.incr('jobIdCounter');
        return jobId.toString();
    }

    async fetchJobData(jobId) {
        const jobData = await this.redisClient.hGetAll(`jobQueue:${this.name}:${jobId}`);
        if (Object.keys(jobData).length === 0) {
            return null;
        }

        // Parse the jobData correctly
        const parsedJobData = {};
        for (const [key, value] of Object.entries(jobData)) {
            try {
                parsedJobData[key] = JSON.parse(value);
            } catch (error) {
                parsedJobData[key] = value; // If JSON.parse fails, store the raw value
            }
        }

        // Reconstruct the job and jobOptions from the parsed data
        const job = parsedJobData.data ? parsedJobData.data : {};
        const jobOptions = parsedJobData.jobOptions ? parsedJobData.jobOptions : {};

        return new Job(job, jobOptions);
    }

    async processJobs(callback, concurrency) {
        await this.ready;
        const semaphore = new Semaphore(concurrency);

        const processJob = async (jobId) => {
            const jobInstance = await this.fetchJobData(jobId);

            if (!jobInstance) {
                console.error(`No job instance found for job ID ${jobId}`);
                return;
            }

            await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, 'status', 'processing');
            await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, 'processedOn', JSON.stringify(new Date()));

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

                await this.handleJob(jobId);
            } catch (error) {
                console.error('Error in job processing:', error);
                await this.failJob(jobId, jobInstance);
            }
        };

        const worker = async () => {
            while (true) {
                try {
                    // Check if token bucket is defined and consume tokens if available
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
                            // No jobs in the queue, waiting...
                            await new Promise(resolve => setTimeout(resolve, 1000));
                        }
                    } else {
                        // Wait for a token to become available
                        await new Promise(resolve => setTimeout(resolve, 100));
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
        } catch (error) {
            console.error('Error handling job:', error);
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
        await this.jobDataUpdater(jobInstance, jobId);
    }

    async jobDataUpdater(job, jobId) {
        try {
            for (let key in job) {
                await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, key.toString(), JSON.stringify(job[key]));
            }
        } catch (error) {
            console.error('Error updating job data:', error);
        }
    }
}

module.exports = Queue;
