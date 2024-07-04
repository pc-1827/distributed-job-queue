const redis = require('redis');
const dotenv = require('dotenv');
const Job = require('./job');

dotenv.config();

class Queue {
    constructor(name, queueOptions) {
        this.name = name;
        this.queueOptions = queueOptions;

        // Initialize redisOptions and redisClient inside the constructor
        this.redisOptions = this.queueOptions.redis;
        this.redisClient = redis.createClient({
            url: `redis://${this.redisOptions.password ? this.redisOptions.password + '@' : ''}${this.redisOptions.host}:${this.redisOptions.port}`
        });

        (async () => {
            await this.redisClient.connect();
        })();
    }

    async addJobs(job, jobOptions) {
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

const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = process.env;
const redisOptions = {
    redis: { host: REDIS_HOST, port: REDIS_PORT},
};

const burgerQueue = new Queue("burger", redisOptions);

const jobs = [...new Array(10)].map((_) => ({
    bun: "🍔",
    cheese: "🧀",
    toppings: ["🍅", "🫒", "🥒", "🌶️"],
}));

jobs.forEach((job) => burgerQueue.addJobs(job, { attempt: 3, repeat: { cron: "10 * * * * *" }}));


burgerQueue.processJobs((job, done) => {
    console.log("Preparing the burger!");
    setTimeout(() => {
        console.log("Burger is ready!");
        done();
    }, 4000);
});
