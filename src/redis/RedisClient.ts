import Redis from "ioredis";

const redis = new Redis({
    host: process.env.REDIS_HOST ?? 'localhost',
    port: Number(process.env.REDIS_PORT ?? 6379),
})

export async function setOnlineUser(userId: string, instanceId: string) {
    await redis.set(`online:${userId}`, instanceId, "EX", 60)
}

export async function expireOnlineStatusUser(userId: string) {
    await redis.expire(`online:${userId}`, 60)
}

export async function disconnectUser(userId: string) {
    await redis.del(`online:${userId}`)
    await redis.set(`last_seen:${userId}`, new Date().toISOString())
}
