import {disconnectUser, expireOnlineStatusUser, setOnlineUser} from "../redis/RedisClient";
import {WebSocketServer} from "ws";
import {clearInterval} from "node:timers";

const wss = new WebSocketServer({port: 8080})

wss.on('connection', async (ws, request) => {
    const userId = request.headers['x-user-id']
    if (!userId || Array.isArray(userId)) {
        ws.terminate()
        return
    }

    let isAlive = true
    const interval = setInterval(() => {
        if (!isAlive) {
            ws.terminate()
            return
        }
        isAlive = false
        ws.ping()
    }, 30_000)

    try {
        await setOnlineUser(userId)
    } catch (error) {
        console.log('Failed to set online status user:', error)
    }

    ws.on('close', async () => {
        clearInterval(interval)
        try {
            await disconnectUser(userId)
        } catch (error) {
            console.log('Failed to disconnect user:', error)
        }
    })

    ws.on('pong', async () => {
        isAlive = true
        try {
            await expireOnlineStatusUser(userId)
        } catch (error) {
            console.log('Failed to expire status user:', error)
        }
    })
})