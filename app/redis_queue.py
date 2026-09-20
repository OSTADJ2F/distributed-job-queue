import time
import uuid
from dataclasses import dataclass
from datetime import datetime

from redis.asyncio import Redis

CLAIM_SCRIPT = """
local scheduled = KEYS[1]
local processing = KEYS[2]
local now = tonumber(ARGV[1])
local deadline = tonumber(ARGV[2])
local batch = tonumber(ARGV[3])
local due = redis.call('ZRANGEBYSCORE', scheduled, '-inf', now, 'LIMIT', 0, batch)
for _, packed in ipairs(due) do
  redis.call('ZREM', scheduled, packed)
  local sep = string.find(packed, ':')
  local priority = string.sub(packed, 1, sep - 1)
  local job_id = string.sub(packed, sep + 1)
  redis.call('RPUSH', KEYS[3 + tonumber(priority)], job_id)
end
for priority = 9, 0, -1 do
  local job_id = redis.call('LPOP', KEYS[3 + priority])
  if job_id then
    redis.call('ZADD', processing, deadline, job_id)
    return job_id
  end
end
return nil
"""

ACK_SCRIPT = """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('SREM', KEYS[2], ARGV[1])
return 1
"""

ENQUEUE_SCRIPT = """
redis.call('LREM', KEYS[4], 0, ARGV[1])
if redis.call('SADD', KEYS[1], ARGV[1]) == 0 then
  return 0
end
if tonumber(ARGV[3]) > tonumber(ARGV[4]) then
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2] .. ':' .. ARGV[1])
else
  redis.call('RPUSH', KEYS[3], ARGV[1])
end
return 1
"""


@dataclass(frozen=True)
class QueueStats:
    ready: int
    scheduled: int
    running: int
    dead_letter: int


class RedisJobQueue:
    def __init__(self, redis: Redis, name: str = "default") -> None:
        self.redis = redis
        self.name = name

    @property
    def prefix(self) -> str:
        return f"queue:{self.name}"

    def ready_key(self, priority: int) -> str:
        return f"{self.prefix}:ready:{priority}"

    @property
    def scheduled_key(self) -> str:
        return f"{self.prefix}:scheduled"

    @property
    def processing_key(self) -> str:
        return f"{self.prefix}:processing"

    @property
    def active_key(self) -> str:
        return f"{self.prefix}:active"

    @property
    def dead_key(self) -> str:
        return f"{self.prefix}:dead"

    async def enqueue(
        self, job_id: uuid.UUID | str, priority: int, run_at: datetime | None = None
    ) -> None:
        job_id_string = str(job_id)
        run_at_timestamp = run_at.timestamp() if run_at is not None else 0
        await self.redis.eval(
            ENQUEUE_SCRIPT,
            4,
            self.active_key,
            self.scheduled_key,
            self.ready_key(priority),
            self.dead_key,
            job_id_string,
            priority,
            run_at_timestamp,
            time.time(),
        )

    async def claim(self, visibility_timeout: int) -> str | None:
        keys = [self.scheduled_key, self.processing_key]
        keys.extend(self.ready_key(priority) for priority in range(10))
        now = time.time()
        result = await self.redis.eval(
            CLAIM_SCRIPT, len(keys), *keys, now, now + visibility_timeout, 100
        )
        if result is None:
            return None
        return result.decode() if isinstance(result, bytes) else str(result)

    async def ack(self, job_id: uuid.UUID | str) -> None:
        await self.redis.eval(ACK_SCRIPT, 2, self.processing_key, self.active_key, str(job_id))

    async def dead_letter(self, job_id: uuid.UUID | str) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.processing_key, str(job_id))
            pipe.srem(self.active_key, str(job_id))
            pipe.rpush(self.dead_key, str(job_id))
            await pipe.execute()

    async def remove(self, job_id: uuid.UUID | str, priority: int) -> None:
        value = str(job_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lrem(self.ready_key(priority), 0, value)
            pipe.zrem(self.processing_key, value)
            pipe.srem(self.active_key, value)
            for member in await self.redis.zrange(self.scheduled_key, 0, -1):
                decoded = member.decode() if isinstance(member, bytes) else str(member)
                if decoded.endswith(f":{value}"):
                    pipe.zrem(self.scheduled_key, decoded)
            await pipe.execute()

    async def release(self, job_id: uuid.UUID | str, priority: int) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.processing_key, str(job_id))
            pipe.sadd(self.active_key, str(job_id))
            pipe.lrem(self.dead_key, 0, str(job_id))
            pipe.rpush(self.ready_key(priority), str(job_id))
            await pipe.execute()

    async def reschedule(self, job_id: uuid.UUID | str, priority: int, run_at: datetime) -> None:
        value = str(job_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.processing_key, value)
            pipe.zadd(self.scheduled_key, {f"{priority}:{value}": run_at.timestamp()})
            await pipe.execute()

    async def is_active(self, job_id: uuid.UUID | str) -> bool:
        return bool(await self.redis.sismember(self.active_key, str(job_id)))

    async def remove_from_dead_letter(self, job_id: uuid.UUID | str) -> None:
        await self.redis.lrem(self.dead_key, 0, str(job_id))

    async def stats(self) -> QueueStats:
        async with self.redis.pipeline(transaction=False) as pipe:
            for priority in range(10):
                pipe.llen(self.ready_key(priority))
            pipe.zcard(self.scheduled_key)
            pipe.zcard(self.processing_key)
            pipe.llen(self.dead_key)
            values = await pipe.execute()
        return QueueStats(
            ready=sum(values[:10]),
            scheduled=values[10],
            running=values[11],
            dead_letter=values[12],
        )

    async def depth(self) -> int:
        stats = await self.stats()
        return stats.ready + stats.scheduled + stats.running

    async def ping(self) -> bool:
        return bool(await self.redis.ping())
