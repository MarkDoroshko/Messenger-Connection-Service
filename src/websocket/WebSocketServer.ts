import { disconnectUser, expireOnlineStatusUser, setOnlineUser } from "../redis/RedisClient";
import { WebSocketServer, WebSocket } from "ws";
import { clearInterval } from "node:timers";
import { INSTANCE_ID, publishMessageSent, startAmqp } from "../amqp/AmqpClient";

const PORT = Number(process.env.PORT ?? 8080)
const GATEWAY_SECRET = process.env.GATEWAY_SECRET ?? ''

const wss = new WebSocketServer({ port: PORT })

console.log(`[connection-service] WebSocket server listening on :${PORT}, instance=${INSTANCE_ID}`)
if (!GATEWAY_SECRET) {
    console.warn('[connection-service] WARNING: GATEWAY_SECRET is empty — connections will be rejected. Set the env var.')
}

const userConnections = new Map<string, Set<WebSocket>>()

function addConn(userId: string, ws: WebSocket) {
    let set = userConnections.get(userId)
    if (!set) { set = new Set(); userConnections.set(userId, set) }
    set.add(ws)
}

function removeConn(userId: string, ws: WebSocket) {
    const set = userConnections.get(userId)
    if (!set) return
    set.delete(ws)
    if (set.size === 0) userConnections.delete(userId)
}

function deliverToUser(userId: string, payload: any) {
    const set = userConnections.get(userId)
    if (!set || set.size === 0) {
        console.warn(`[connection-service] deliver: user ${userId} not connected to this instance`)
        return
    }
    const data = JSON.stringify(payload)
    for (const ws of set) {
        if (ws.readyState === ws.OPEN) ws.send(data)
    }
}

startAmqp((payload) => {
    if (payload?.type === 'message' && typeof payload.to === 'string') {
        deliverToUser(payload.to, payload)
    } else {
        console.warn('[connection-service] unknown deliver payload:', payload)
    }
}).catch(err => {
    console.error('[connection-service] amqp fatal:', err)
    process.exit(1)
})

wss.on('connection', async (ws, request) => {
    const gatewaySecret = request.headers['x-gateway-secret']
    const userIdHeader = request.headers['x-user-id']

    if (!GATEWAY_SECRET || gatewaySecret !== GATEWAY_SECRET) {
        console.warn('[connection-service] rejected: bad/missing X-Gateway-Secret')
        ws.close(4401, 'unauthorized')
        return
    }
    if (!userIdHeader || Array.isArray(userIdHeader)) {
        console.warn('[connection-service] rejected: missing X-User-Id')
        ws.close(4400, 'no user id')
        return
    }
    const userId = userIdHeader

    let isAlive = true
    const interval = setInterval(() => {
        if (!isAlive) {
            ws.terminate()
            return
        }
        isAlive = false
        try { ws.ping() } catch {}
    }, 30_000)

    try {
        await setOnlineUser(userId, INSTANCE_ID)
        addConn(userId, ws)
        console.log(`[connection-service] user ${userId} online (instance=${INSTANCE_ID})`)
    } catch (error) {
        console.log('Failed to set online status user:', error)
    }

    ws.on('message', (raw) => {
        let parsed: any
        try {
            parsed = JSON.parse(raw.toString())
        } catch {
            ws.send(JSON.stringify({ type: 'error', error: 'invalid_json' }))
            return
        }
        if (parsed?.type === 'message' && typeof parsed.to === 'string' && typeof parsed.content === 'string') {
            const ok = publishMessageSent({
                from: userId,
                to: parsed.to,
                content: parsed.content,
                clientMessageId: parsed.clientMessageId,
            })
            ws.send(JSON.stringify({
                type: 'ack',
                clientMessageId: parsed.clientMessageId,
                accepted: ok,
            }))
        } else if (parsed?.type === 'ping') {
            ws.send(JSON.stringify({ type: 'pong' }))
        } else {
            ws.send(JSON.stringify({ type: 'error', error: 'unknown_type' }))
        }
    })

    ws.on('close', async () => {
        clearInterval(interval)
        removeConn(userId, ws)
        try {
            // Only flip presence if this was the user's last connection on this instance.
            if (!userConnections.has(userId)) {
                await disconnectUser(userId)
                console.log(`[connection-service] user ${userId} offline`)
            }
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
