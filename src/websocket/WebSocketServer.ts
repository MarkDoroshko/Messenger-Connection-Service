import {disconnectUser, expireOnlineStatusUser, setOnlineUser} from "../redis/RedisClient";
import {WebSocketServer} from "ws";
import {clearInterval} from "node:timers";

const PORT = Number(process.env.PORT ?? 8080)
const GATEWAY_SECRET = process.env.GATEWAY_SECRET ?? ''

const wss = new WebSocketServer({port: PORT})

console.log(`[connection-service] WebSocket server listening on :${PORT}`)
if (!GATEWAY_SECRET) {
    console.warn('[connection-service] WARNING: GATEWAY_SECRET is empty — connections will be rejected. Set the env var.')
}

wss.on('connection', async (ws, request) => {
    // X-User-Id и X-Gateway-Secret выставляются только Gateway (nginx) после auth_request.
    // Прямые клиентские заголовки игнорируем: соединение принимается только если совпал
    // shared secret, который знают только Gateway и этот сервис.
    const gatewaySecret = request.headers['x-gateway-secret']
    const userId = request.headers['x-user-id']

    if (!GATEWAY_SECRET || gatewaySecret !== GATEWAY_SECRET) {
        console.warn('[connection-service] rejected: bad/missing X-Gateway-Secret')
        ws.close(4401, 'unauthorized')
        return
    }
    if (!userId || Array.isArray(userId)) {
        console.warn('[connection-service] rejected: missing X-User-Id')
        ws.close(4400, 'no user id')
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
        console.log(`[connection-service] user ${userId} online`)
    } catch (error) {
        console.log('Failed to set online status user:', error)
    }

    ws.on('close', async () => {
        clearInterval(interval)
        try {
            await disconnectUser(userId)
            console.log(`[connection-service] user ${userId} offline`)
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
