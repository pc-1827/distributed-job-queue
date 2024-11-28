-- fetch_job.lua
local waitingQueue = KEYS[1]
local activeQueue = KEYS[2]
local jobDataKeyPrefix = ARGV[1]  -- e.g., "jobQueue:myQueue:"

local jobEntry = redis.call('zrange', waitingQueue, 0, 0)
if next(jobEntry) ~= nil then
    local jobId = jobEntry[1]
    -- Remove the job from the waiting queue
    redis.call('zrem', waitingQueue, jobId)

    -- Add the job ID to the active queue
    redis.call('lpush', activeQueue, jobId)

    -- Fetch job data
    local jobDataKey = jobDataKeyPrefix .. jobId
    local jobData = redis.call('hget', jobDataKey, 'data')

    -- Return job ID and Job Data
    return {jobId, jobData}
end
return nil

-- -- fetch_job.lua
-- local job = redis.call('zrange', KEYS[1], 0, 0)
-- if next(job) ~= nil then
--     -- Remove the job from the waiting queue
--     redis.call('zrem', KEYS[1], job[1])

--     -- Publish update on the waiting channel
--     redis.call('publish', 'waiting', 'change')

--     -- Add the job to the active queue
--     redis.call('lpush', KEYS[2], job[1])

--     -- Publish update on the active channel
--     redis.call('publish', 'active', 'change')
--     return job[1]
-- end
-- return nil
