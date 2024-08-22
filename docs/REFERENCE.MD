## Queue Instantiation

To create a queue, instantiate the `Queue` class with a name and options. The options allow you to configure Redis connection details and job rate limiting.

```js
const Queue = require('distributed-job-queue');
const dotenv = require('dotenv');

dotenv.config();

const { REDIS_HOST, REDIS_PORT, REDIS_PASSWORD } = process.env;

const queueOptions = {
    redis: {
        host: REDIS_HOST,       // Redis server host
        port: REDIS_PORT,       // Redis server port
        password: REDIS_PASSWORD, // Redis server password
    },
    limit: {
        max: 5,    // Maximum number of jobs processed in the given duration
        duration: 100, // Duration in milliseconds for rate limiting
    },
};

const myFirstQueue = new Queue("myFirstQueue", queueOptions);
```

### Queue Options

- `redis`: An object specifying the Redis connection details.
   - `host`: The Redis server host.
   - `port`: The Redis server port.
   - `password`: The Redis server password(optional).

- `limit`: An object specifying the rate-limiting options for job processing.
   - `max`: The maximum number of jobs that can be processed within the specified duration.
   - `duration`: The duration (in milliseconds) during which the maximum number of jobs can be processed.

## Adding Jobs to the Queue

Use the `addJobs` method to create a new job and add it to the queue. If the queue is empty the job will be executed directly, otherwise it will be placed in the queue and executed as soon as possible.  Each job is an object representing the task you want to perform. You can have multiple producers which can add Jobs to the queue.

First argument provided to the `addJobs` method is a data object which contains the necessary details for the jobs. Second argument is a jobOptions object for the job, which includes number of attempts, priority of the job and if the job needs to be repeated.

```js
const myFirstQueue = new Queue("myFirstQueue", queueOptions);

const job = {
    data: data,
    value: value,
    //other stuff
};

myFirstQueue.addJobs(job, { attempts: 2, repeat: { cron: "1 * * * * *" }, priority : 1});
```

### Job Stauses

Jobs in the queue can have different statuses depending on their current state in the processing pipeline. Understanding these statuses can help you manage and monitor your job queue more effectively.

- `Waiting`: The job has been added to the queue and is waiting to be processed. This status is typical when there are more jobs in the queue than there are available workers.

- `Processing`: The job is currently being processed by a worker. When a job is in this state, the worker is actively executing the job’s logic.

- `Completed`: The job has been successfully processed and completed without any errors. Once a job reaches this status, it is typically removed from the queue unless you have configured the queue to retain completed jobs for logging or auditing purposes.

- `Failed`: The job has been processed but encountered an error during execution. If a job fails, it may be retried automatically depending on the `attempts` option provided when the job was added. If all retries are exhausted, the job will remain in the failed state.

- `Delayed`: The job is scheduled to be processed at a later time. This can happen if the job was added with a `repeat` option or if the job needs to be delayed due to other constraints.

### Job Options
- `attempts`: The number of times a job should be retried if it fails. Default is `1`.
- `priority`: The priority of the job. Lower numbers indicate higher priority. Jobs with the same priority are processed in the order they were added. Defaults is `999`.
- `repeat`: An optional object specifying the repetition pattern for the job.
  - `cron`: A cron string defining the repetition schedule.

## Processing Jobs in the Queue
The `processJobs` method is used to define how jobs in the queue should be processed. You provide a callback function that will handle each job, and you have to specify the concurrency(number of jobs executed at a time by a consumer) level. You can have multiple consumers for a queue, the  `distributed-job-queue` npm package equally distributes jobs among the consumers for maximum efficiency.

The `done` callback function is used to signal the completion of a job. It accepts an optional error parameter:

- If `done()` is called without arguments, the job is considered successful.
- If `done(error)` is called with an error, the job is considered failed and will be retried if the `attempts` option was set.

```js
myFirstQueue.processJobs((job, done) => {
    setTimeout(() => {
        try {
            if (Math.random() < 0.5) {
                throw new Error("Job Failed");
            }
            console.log("Job Completed");
            done(); // Mark the job as successfully completed
        } catch (error) {
            done(error); // Mark the job as failed
        }
    }, 1000);
}, 5);
```
