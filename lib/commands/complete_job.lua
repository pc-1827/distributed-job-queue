-- job_complete.lua
local jobId = ARGV[1]
local completedTime = ARGV[2]
local activeQueue = KEYS[1]
local completedQueue = KEYS[2]
local jobDataKey = KEYS[3]

-- Remove the job from active queue
redis.call('lrem', KEYS[1], 1, ARGV[1])

-- Update job status to completed
redis.call('hset', KEYS[3], 'status', 'completed')

-- Add the job to completed queue
redis.call('lpush', KEYS[2], ARGV[1])

-- Set the completed time
redis.call('hset', KEYS[3], 'completedOn', ARGV[2])

return true
