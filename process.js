const Queue = require("./lib/queue")
const dotenv = require('dotenv');

dotenv.config();

const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = process.env;
const queueOptions = {
    redis: { host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASSWORD },
};

const burgerQueue = new Queue("burger", queueOptions);

console.log("Consumers are ready to process the jobs!");

burgerQueue.processJobs((job, done) => {
    //console.log("Preparing the burger!");
    setTimeout(() => {
        try {
            if (Math.random() < 0.5) {
                throw new Error("Failed to prepare the burger!");
            }
            //console.log("Burger is ready!");
            done();
        } catch (error) {
            done(error);
        }
    }, 1000);
}, 5);
