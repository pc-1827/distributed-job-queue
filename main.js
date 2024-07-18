const Queue = require("./lib/queue")
const dotenv = require('dotenv');

dotenv.config();

const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = process.env;
const queueOptions = {
    redis: { host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASSWORD },
};

const burgerQueue = new Queue("burger", queueOptions);

const jobs = [...new Array(10)].map(() => ({
    bun: "🍔",
    cheese: "🧀",
    toppings: ["🍅", "🫒", "🥒", "🌶️"],
}));

let priority = 10;

for (const job of jobs) {
    burgerQueue.addJobs(job, { attempts: 2, repeat: { cron: "10 * * * * *" }, priority : priority--
    });
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
}, 5);
