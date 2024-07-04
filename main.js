const Queue = require("./lib/queue")
const dotenv = require('dotenv');

dotenv.config();

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
