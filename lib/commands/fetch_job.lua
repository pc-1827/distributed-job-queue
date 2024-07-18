-- fetch_job.lua
local job = redis.call('zrange', KEYS[1], 0, 0)
if next(job) ~= nil then
    redis.call('zrem', KEYS[1], job[1])
    redis.call('lpush', KEYS[2], job[1])
    return job[1]
end
return nil
