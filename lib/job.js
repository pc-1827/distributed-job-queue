class Job {
    constructor(data, jobOptions) {
        this.data = data;
        this.jobOptions = jobOptions;
        this.status = 'waiting';
        this.createdOn = new Date();
        this.processedOn = null;
        this.completedOn = null;
    }

    async isProcessing() {
        this.status = 'processing';
        this.processedOn = new Date();
    }

    async isCompleted() {
        this.status = 'completed';
        this.completedOn = new Date();
    }

    async isFailed() {
        this.status = 'failed';
    }
}

// const jobData = {
//     bun: "🍔",
//     cheese: "🧀",
//     toppings: ["🍅", "🫒", "🥒", "🌶️"],
// }

// const jobOptions = {
//     attempt: 3,
//     repeat: { cron: "10 * * * * *" },
// }

// const job1 = new Job(jobData, jobOptions);
// console.log(job1);

module.exports = Job;
