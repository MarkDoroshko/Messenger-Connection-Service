import Redis from "ioredis";

const redis = new Redis();  // TODO: Сделать класс и передавать объект в конструктор

export async function setOnlineUser(userId: string) {
    await redis.set(`online:${userId}`, "true", "EX", 60)
}

export async function expireOnlineStatusUser(userId: string) {
    await redis.expire(`online:${userId}`, 60)
}

export async function disconnectUser(userId: string) {
    await redis.del(`online:${userId}`)
    await redis.set(`last_seen:${userId}`, Date.now().toString())
}