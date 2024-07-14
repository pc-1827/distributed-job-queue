const Queue = require("./lib/queue")
const dotenv = require('dotenv');

dotenv.config();

const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = process.env;
const redisOptions = {
    redis: { host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASSWORD },
};

const burgerQueue = new Queue("burger", redisOptions);

const jobs = [...new Array(10)].map(() => ({
    bun: "🍔",
    cheese: "🧀",
    toppings: ["🍅", "🫒", "🥒", "🌶️"],
}));

for (const job of jobs) {
    burgerQueue.addJobs(job, { attempts: 2, repeat: { cron: "10 * * * * *" } });
}

burgerQueue.processJobs((job, done) => {
    //console.log("Preparing the burger!");
    setTimeout(() => {
        try {
            if (Math.random() < 0.75) {
                throw new Error("Failed to prepare the burger!");
            }
            //console.log("Burger is ready!");
            done();
        } catch (error) {
            done(error);
        }
    }, 1000);
}, 1);
