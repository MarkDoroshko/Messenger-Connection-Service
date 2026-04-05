import Redis from "ioredis";

const redis = new Redis();

function setOnlineUser(userId: number) {
    redis.set(`online:${userId}`, "true", "EX", 60)
}

function expireOnlineStatusUser(userId: number) {
    redis.expire(`online:${userId}`, 60)
}

function disconnectUser(userId: number) {
    redis.del(`online:${userId}`)
}