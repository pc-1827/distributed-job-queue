const redis = require('redis');
const Job = require('distributed-job-queue/lib/job');
const TokenBucket = require('distributed-job-queue/lib/token');
const { Semaphore } = require('async-mutex');
const fs = require('fs');
const path = require('path');
const scheduleRepeatedJob = require('distributed-job-queue/lib/helpers/scheduleRepeatedJob');
const fetchJobData = require('distributed-job-queue/lib/helpers/fetchJobData');
const jobDataUpdater = require('distributed-job-queue/lib/helpers/jobDataUpdater');

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

    async addJobs(jobData, jobOptions) {
        await this.ready;
        try {
            const newJob = new Job(jobData, jobOptions);
            const newJobId = await this.generateJobId();
            const jobKey = `jobQueue:${this.name}:${newJobId}`;
            const priority = jobOptions.priority || 999;

            // Prepare job data
            const jobDataString = JSON.stringify({
                data: newJob.data,
                jobOptions: newJob.jobOptions,
                status: newJob.status,
                createdOn: newJob.createdOn,
                processedOn: newJob.processedOn,
                completedOn: newJob.completedOn,
            });

            await this.redisClient.hSet(jobKey, 'data', jobDataString);
            await this.redisClient.zAdd(`jobQueue:${this.name}:waiting`, [{ score: priority, value: newJobId.toString() }]);

            // Publish change notification
            await this.redisClient.publish("waiting", "change");
        } catch (error) {
            console.error('Failed to add job:', error);
        }
    }

    async generateJobId() {
        const jobId = await this.redisClient.incr('jobIdCounter');
        return jobId.toString();
    }

    async processJobs(callback, concurrency) {
        await this.ready;
        const semaphore = new Semaphore(concurrency);

        const processJob = async (jobId, jobInstance) => {
            if (!jobInstance) {
                console.error(`No job instance found for job ID ${jobId}`);
                return;
            }

            // Update 'status' and 'processedOn' in the job data
            jobInstance.status = 'processing';
            jobInstance.processedOn = new Date();

            // Write back the updated job data in one Redis call
            const jobKey = `jobQueue:${this.name}:${jobId}`;
            const jobDataString = JSON.stringify({
                data: jobInstance.data,
                jobOptions: jobInstance.jobOptions,
                status: jobInstance.status,
                createdOn: jobInstance.createdOn,
                processedOn: jobInstance.processedOn,
                completedOn: jobInstance.completedOn,
            });
            await this.redisClient.hSet(jobKey, 'data', jobDataString);

            // Optionally publish changes if needed
            // await this.redisClient.publish("waiting", "change");

            try {
                await new Promise((resolve) => {
                    const done = async (err) => {
                        if (err) {
                            if (jobInstance.jobOptions.attempts && jobInstance.jobOptions.attempts > 1) {
                                await this.retryJob(jobId, jobInstance);
                                //console.log('Job failed, trying again:', jobId);
                            } else {
                                await this.failJob(jobId);
                                //console.log('Job failed:', jobId);
                            }
                        } else {
                            await this.completeJob(jobId);
                            //console.log('Job completed:', jobId);
                        }
                        resolve();
                    };
                    // Pass the job instance to the callback for processing
                    callback(jobInstance.data, done);
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
                        // Execute the updated fetchJob Lua script
                        const result = await this.redisClient.eval(this.luaScripts.fetchJob, {
                            keys: [
                                `jobQueue:${this.name}:waiting`,
                                `jobQueue:${this.name}:active`
                            ],
                            arguments: [`jobQueue:${this.name}:`] // Pass the job data key prefix
                        });

                        if (result) {
                            const [jobId, jobDataString] = result;

                            // Parse the job data
                            let jobInstance;
                            try {
                                const jobObject = JSON.parse(jobDataString);
                                jobInstance = new Job(jobObject.data, jobObject.jobOptions);
                                jobInstance.status = jobObject.status;
                                jobInstance.createdOn = new Date(jobObject.createdOn);
                                jobInstance.processedOn = jobObject.processedOn ? new Date(jobObject.processedOn) : null;
                                jobInstance.completedOn = jobObject.completedOn ? new Date(jobObject.completedOn) : null;
                            } catch (error) {
                                console.error('Error parsing job data:', error);
                                // Handle parsing error by failing the job
                                await this.failJob(jobId);
                                continue;
                            }

                            await semaphore.acquire();
                            processJob(jobId, jobInstance).finally(() => semaphore.release());
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

    // async processJobs(callback, concurrency) {
    //     await this.ready;
    //     const semaphore = new Semaphore(concurrency);

    //     const processJob = async (jobId) => {
    //         const jobInstance = await fetchJobData(this.redisClient, this.name, jobId);

    //         if (!jobInstance) {
    //             console.error(`No job instance found for job ID ${jobId}`);
    //             return;
    //         }

    //         await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, 'status', 'processing');
    //         await this.redisClient.hSet(`jobQueue:${this.name}:${jobId}`, 'processedOn', JSON.stringify(new Date()));
    //         await this.redisClient.publish("waiting", "change");

    //         try {
    //             await new Promise((resolve) => {
    //                 const done = async (err) => {
    //                     if (err) {
    //                         if (jobInstance.jobOptions.attempts && jobInstance.jobOptions.attempts > 1) {
    //                             await this.retryJob(jobId, jobInstance);
    //                             console.log('Job failed, trying again:', jobId);
    //                         } else {
    //                             await this.failJob(jobId);
    //                             console.log('Job failed:', jobId);
    //                         }
    //                     } else {
    //                         await this.completeJob(jobId);
    //                         console.log('Job completed:', jobId);
    //                     }
    //                     resolve();
    //                 };
    //                 callback(jobId, done);
    //             });

    //         } catch (error) {
    //             console.error('Error in job processing:', error);
    //             await this.failJob(jobId, jobInstance);
    //         }
    //     };

    //     const worker = async () => {
    //         while (true) {
    //             try {
    //                 if (!this.tokenBucket || this.tokenBucket.consume()) {
    //                     const job = await this.redisClient.eval(this.luaScripts.fetchJob, {
    //                         keys: [
    //                             `jobQueue:${this.name}:waiting`,
    //                             `jobQueue:${this.name}:active`
    //                         ],
    //                         arguments: []
    //                     });

    //                     if (job) {
    //                         const jobId = job.toString();
    //                         await semaphore.acquire();
    //                         processJob(jobId).finally(() => semaphore.release());
    //                     } else {
    //                         await new Promise(resolve => setTimeout(resolve, 1000));
    //                     }
    //                 } else {
    //                     await new Promise(resolve => setTimeout(resolve, 100));
    //                 }
    //             } catch (error) {
    //                 console.error('Error fetching job from the queue:', error);
    //             }
    //         }
    //     };

    //     for (let i = 0; i < concurrency; i++) {
    //         worker();
    //     }
    // }

    async completeJob(jobId) {
        const jobKey = `jobQueue:${this.name}:${jobId}`;
        const jobDataString = await this.redisClient.hGet(jobKey, 'data');
        if (jobDataString) {
            let jobObject = JSON.parse(jobDataString);
            jobObject.status = 'completed';
            jobObject.completedOn = new Date();
            await this.redisClient.hSet(jobKey, 'data', JSON.stringify(jobObject));
        }

        // Move the job from active to completed queue
        await this.redisClient.eval(this.luaScripts.completeJob, {
            keys: [
                `jobQueue:${this.name}:active`,
                `jobQueue:${this.name}:completed`
            ],
            arguments: [jobId.toString()]
        });
    }

    async failJob(jobId) {
        const jobKey = `jobQueue:${this.name}:${jobId}`;
        const jobDataString = await this.redisClient.hGet(jobKey, 'data');
        if (jobDataString) {
            let jobObject = JSON.parse(jobDataString);
            jobObject.status = 'failed';
            await this.redisClient.hSet(jobKey, 'data', JSON.stringify(jobObject));
        }

        // Move the job from active to failed queue
        await this.redisClient.eval(this.luaScripts.failJob, {
            keys: [
                `jobQueue:${this.name}:active`,
                `jobQueue:${this.name}:failed`
            ],
            arguments: [jobId.toString()]
        });
    }

    async retryJob(jobId, jobInstance) {
        const jobKey = `jobQueue:${this.name}:${jobId}`;
        // Update job data
        jobInstance.status = 'waiting';
        jobInstance.processedOn = null;
        jobInstance.attempts--; // Ensure this field exists
        await jobDataUpdater(this.redisClient, this.name, jobInstance, jobId);

        // Move the job from active to waiting queue
        await this.redisClient.eval(this.luaScripts.retryJob, {
            keys: [
                `jobQueue:${this.name}:active`,
                `jobQueue:${this.name}:waiting`
            ],
            arguments: [jobId.toString()]
        });
    }
}

module.exports = Queue;
