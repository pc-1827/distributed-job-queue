class Job {
    constructor(data, jobOptions) {
        this.data = data;
        this.jobOptions = jobOptions;
        this.status = 'waiting';
        this.createdOn = new Date();
        this.processedOn = null;
        this.completedOn = null;
    }
}

module.exports = Job;
