-- retry_job.lua
local activeQueue = KEYS[1]
local waitingQueue = KEYS[2]
local jobId = ARGV[1]

-- Remove the job from active queue
redis.call('lrem', activeQueue, 1, jobId)

-- Add the job back to waiting queue with original priority
redis.call('zadd', waitingQueue, 999, jobId)

return true

-- local jobId = ARGV[1]
-- local activeQueue = KEYS[1]
-- local failedQueue = KEYS[2]
-- local waitQueue = KEYS[3]
-- local jobDataKey = KEYS[4]

-- -- Remove the job from active queue
-- redis.call('lrem', KEYS[1], 1, ARGV[1])

-- -- Publish update on the active channel
-- redis.call('publish', 'active', 'change')

-- -- Add the job to wait queue
-- redis.call('zadd', KEYS[3], 999, ARGV[1])

-- -- Update job status to failed
-- redis.call('hset', KEYS[4], 'status', 'failed')

-- -- Publish change to the waiting channel
-- redis.call('publish', 'waiting', 'change')

-- return true
