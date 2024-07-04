const redis = require('redis');
const Job = require('./job');
const { promisify } = require('util');

const sleep = promisify(setTimeout);

class Queue {
    constructor(name, queueOptions) {
        this.name = name;
        this.queueOptions = queueOptions;

        // Initialize redisOptions and redisClient inside the constructor
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
            await this.jobDataUpdater(newJob, jobId);
            await this.redisClient.lPush(this.name, JSON.stringify(jobId));
            console.log('Job added to the queue:', jobId);
        } catch (error) {
            console.error('Error adding job to the queue:', error);
        }
    }

    async generateJobId() {
        const jobId = await this.redisClient.incr('jobIdCounter');
        return jobId;
    }

    async processJobs(callback) {
        await this.ready;
        try {
            while (true) {
                const job = await this.redisClient.rPop(this.name);
                if (job) {
                    const parsedJob = JSON.parse(job);
                    this.processJob(parsedJob);
                    await new Promise((resolve, reject) => {
                        const done = (err) => {
                            if (err) {
                                this.failJob(parsedJob);
                                reject(err);
                            } else {
                                this.completeJob(parsedJob);
                                resolve();
                            }
                        };
                        callback(parsedJob, done);
                    });

                    await this.handleJob(parsedJob);
                } else {
                    console.log('No jobs in the queue, waiting...');
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }
        } catch (error) {
            console.error('Error processing jobs:', error);
        }
    }

    async handleJob(job) {
        try {
            // Simulate job processing time
            await new Promise(resolve => setTimeout(resolve, 2000));
            console.log('Job processed:', job);
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

    async completeJob(jobId) {
        await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, JSON.stringify('status'), JSON.stringify('completed'));
        await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, JSON.stringify('completedOn'), JSON.stringify(new Date()));
    }

    async processJob(jobId) {
        await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, JSON.stringify('status'), JSON.stringify('processing'));
        await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, JSON.stringify('processedOn'), JSON.stringify(new Date()));
    }

    async failJob(jobId) {
        await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, JSON.stringify('status'), JSON.stringify('failed'));
    }
}

module.exports = Queue;
