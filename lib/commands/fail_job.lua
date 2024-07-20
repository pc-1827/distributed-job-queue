-- job_fail.lua
local jobId = ARGV[1]
local activeQueue = KEYS[1]
local failedQueue = KEYS[2]
local jobDataKey = KEYS[3]

-- Remove the job from active queue
redis.call('lrem', KEYS[1], 1, ARGV[1])

-- Update job status to failed
redis.call('hset', KEYS[3], 'status', 'failed')

-- Add the job to failed queue
redis.call('lpush', KEYS[2], ARGV[1])

return true
