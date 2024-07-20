const redis = require('redis');
const Job = require('./job');
const { Semaphore } = require('async-mutex');
const fs = require('fs');
const path = require('path');
const cronParser = require('cron-parser');

class Queue {
    constructor(name, queueOptions) {
        this.name = name;
        this.queueOptions = queueOptions;
        this.jobsMap = new Map();

        this.redisOptions = this.queueOptions.redis;
        this.redisClient = null;
        this.ready = this.init();

        this.luaScripts = {
            fetchJob : fs.readFileSync(path.join(__dirname, 'commands/fetch_job.lua'), 'utf8'),
            completeJob : fs.readFileSync(path.join(__dirname, 'commands/complete_job.lua'), 'utf8'),
            retryJob : fs.readFileSync(path.join(__dirname, 'commands/retry_job.lua'), 'utf8'),
            failJob : fs.readFileSync(path.join(__dirname, 'commands/fail_job.lua'), 'utf8')
        };
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

            if (jobOptions.repeat && jobOptions.repeat.cron) {
                this.scheduleRepeatedJob(job, jobId, jobOptions);
            }
        } catch (error) {
            console.error('Error adding job to the queue:', error);
        }
    }

    async scheduleRepeatedJob(job, jobId, jobOptions) {
        try {
            const interval = cronParser.parseExpression(jobOptions.repeat.cron);
            const nextRun = interval.next().getTime();
            const delay = nextRun - Date.now();

            const newJob = new Job(job, jobOptions);
            const newJobId = await this.generateJobId();
            this.jobsMap.set(newJobId, newJob);
            await this.jobDataUpdater(newJob, newJobId);

            await this.redisClient.lPush(`jobQueue:${this.name}:delayed`, newJobId.toString());

            setTimeout(async () => {
                await this.redisClient.lRem(`jobQueue:${this.name}:delayed`, 0, newJobId.toString());

                const priority = jobOptions.priority || 999;
                await this.redisClient.zAdd(`jobQueue:${this.name}:waiting`, [{ score: priority, value: newJobId.toString() }]);

                console.log('Repeated job added to the queue:', newJobId);

                // Schedule the next run
                this.scheduleRepeatedJob(job, newJobId, jobOptions);
            }, delay);
        } catch (error) {
            console.error('Error scheduling repeated job:', error);
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
