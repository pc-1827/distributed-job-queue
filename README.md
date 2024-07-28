# Distributed Job Queue

### What is Distributed Job Queue?

Distributed Job Queue is a Node.js library that provides a fast and robust job processing system based on Redis.

While it is possible to implement queues using raw Redis commands, this library offers an API that abstracts away the low-level details and enhances Redis's basic functionality. This allows you to handle more complex use cases with ease.

This project is heavily inspired by [Bull](https://github.com/OptimalBits/bull).


### Getting Started

Distributed Job Queue is available as a public npm package and can be installed using npm

```bash
$ npm install distributed-job-queue --save

```

### Queues

A queue is simply created by instantiating a Queue instance:

```js
const myFirstQueue = new Queue('my-first-queue');
```

A queue instance can normally have 2 main roles: A job producer and job consumer.

The producer and consumer can divided into several instances. A given queue, always referred to by its instantiation name ( my-first-queue in the example above ), can have many producers and consumers. An important aspect is that producers can add jobs to a queue even if there are no consumers available at that moment: queues provide asynchronous communication, which is one of the features that makes them so powerful.

Conversely, you can have one or more workers consuming jobs from the queue, which will consume the jobs in a given order: FIFO or according to priorities.

### Producers

A job producer is a Node.js program that adds jobs to a queue:

```js
const jobData = {
    task: 'sendEmail',
    to: 'user@example.com',
    subject: 'Welcome!',
    body: 'Hello, welcome to our service!',
};

const jobOptions = {
    priority: 1, // Lower number means higher priority
    attempts: 3, // Number of retry attempts if the job fails
};

myFirstQueue.addJobs(jobData, jobOptions)
```

A job is simply a JavaScript object that needs to be serializable (i.e., JSON stringify-able) to be stored in Redis. You can also provide an options object with additional job settings.

### Consumers

A consumer, or worker, is a Node.js program that defines a process function to handle jobs:

```js
burgerQueue.processJobs((job, done) => {
   doSomething(job)
}, 5);
```

The process function is called whenever the worker is idle and there are jobs to process. The queue could have many jobs waiting, and the process function will be busy processing them one by one until all are completed.
