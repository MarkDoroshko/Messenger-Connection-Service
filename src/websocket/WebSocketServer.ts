import {disconnectUser, expireOnlineStatusUser, setOnlineUser} from "../redis/RedisClient";
import {WebSocketServer} from "ws";

const wss = new WebSocketServer({port: 8080})

wss.on('connection', async (ws, request) => {
    const userId = request.headers['x-user-id']
    if (!userId || Array.isArray(userId)) {
        ws.close()
        return
    }

    try {
        await setOnlineUser(userId)
    } catch (error) {
        console.log('Failed to set online status user:', error)
    }

    ws.on('close', async () => {
        try {
            await disconnectUser(userId)
        } catch (error) {
            console.log('Failed to disconnect user:', error)
        }
    })

    ws.on('pong', async () => {
        try {
            await expireOnlineStatusUser(userId)
        } catch (error) {
            console.log('Failed to expire status user:', error)
        }
    })
})