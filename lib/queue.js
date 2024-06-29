const redis = require('redis');
const dotenv = require('dotenv');

class Queue {
    constructor(name, queueOptions) {
        this.name = name;
        this.queueOptions = queueOptions;

        // Move the initialization of redisOptions and redisClient inside the constructor
        this.redisOptions = this.queueOptions.redis;
        this.redisClient = redis.createClient({
            url: `redis://:${this.redisOptions.password}@${this.redisOptions.host}:${this.redisOptions.port}`
        });
        (async () => {
            await this.redisClient.connect();
        })();
    }

    async addJobs(job) {
        //await this.redisClient.connect();
        await this.redisClient.lPush(this.name, JSON.stringify(job));
        console.log('Job added to the queue:', job);
    }

    async processJobs(callback) {
        //await this.redisClient.connect();
        while (true) {
            const job = await this.redisClient.rPop(this.name);
            if (job) {
                const parsedJob = JSON.parse(job);
                await callback(parsedJob)
                // Simulate job processing
                await this.handleJob(parsedJob);
            } else {
                console.log('No jobs in the queue, waiting...');
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    async handleJob(job) {
        // Simulate job processing time
        await new Promise(resolve => setTimeout(resolve, 2000));
        console.log('Job processed:', job);
    }
}

dotenv.config();
const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = process.env;
const redisOptions = {
    redis: { host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASSWORD },
};

const burgerQueue = new Queue("burger", redisOptions);


burgerQueue.addJobs({
    bun: "🍔",
    cheese: "🧀",
    toppings: ["🍅", "🫒", "🥒", "🌶️"],
});

burgerQueue.processJobs(async (job) => {
    console.log("Preparing the burger!")
    await new Promise(resolve => setTimeout(resolve, 4000));
    console.log("Burger is ready!");
});
