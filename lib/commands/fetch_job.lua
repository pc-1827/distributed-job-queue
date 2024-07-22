-- fetch_job.lua
local job = redis.call('zrange', KEYS[1], 0, 0)
if next(job) ~= nil then
    -- Remove the job from the waiting queue
    redis.call('zrem', KEYS[1], job[1])

    -- Publish update on the waiting channel
    redis.call('publish', 'waiting', 'change')

    -- Add the job to the active queue
    redis.call('lpush', KEYS[2], job[1])

    -- Publish update on the active channel
    redis.call('publish', 'active', 'change')
    return job[1]
end
return nil
