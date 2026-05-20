import amqp, { Channel, ChannelModel } from 'amqplib'

export const EXCHANGE = 'messenger'
export const RK_MESSAGE_SENT = 'message.sent'
export const RK_DELIVER_PREFIX = 'deliver.'

export const INSTANCE_ID = process.env.INSTANCE_ID ?? `conn-${Math.random().toString(36).slice(2, 8)}`

let conn: ChannelModel
let channel: Channel
type DeliverHandler = (payload: any) => void
let deliverHandler: DeliverHandler | null = null

async function connectWithRetry(url: string, attempts = 30, delayMs = 2000): Promise<ChannelModel> {
    let lastErr: unknown
    for (let i = 0; i < attempts; i++) {
        try {
            return await amqp.connect(url)
        } catch (e) {
            lastErr = e
            console.log(`[connection-service] amqp connect retry ${i + 1}/${attempts}: ${(e as Error).message}`)
            await new Promise(r => setTimeout(r, delayMs))
        }
    }
    throw lastErr
}

export async function startAmqp(onDeliver: DeliverHandler) {
    deliverHandler = onDeliver
    const url = process.env.AMQP_URL ?? 'amqp://guest:guest@rabbitmq:5672'
    conn = await connectWithRetry(url)
    channel = await conn.createChannel()

    await channel.assertExchange(EXCHANGE, 'topic', { durable: true })

    const queueName = `connection.${INSTANCE_ID}`
    await channel.assertQueue(queueName, { durable: false, autoDelete: true, exclusive: false })
    await channel.bindQueue(queueName, EXCHANGE, RK_DELIVER_PREFIX + INSTANCE_ID)

    await channel.prefetch(32)
    console.log(`[connection-service] amqp ready, instance=${INSTANCE_ID}, listening ${RK_DELIVER_PREFIX}${INSTANCE_ID}`)

    await channel.consume(queueName, (msg) => {
        if (!msg) return
        try {
            const payload = JSON.parse(msg.content.toString())
            deliverHandler?.(payload)
            channel.ack(msg)
        } catch (err) {
            console.error('[connection-service] deliver consume failed:', err)
            channel.nack(msg, false, false)
        }
    })
}

export function publishMessageSent(payload: { from: string; to: string; content: string; clientMessageId?: string }) {
    if (!channel) {
        console.warn('[connection-service] amqp channel not ready, dropping message')
        return false
    }
    return channel.publish(EXCHANGE, RK_MESSAGE_SENT, Buffer.from(JSON.stringify(payload)), {
        persistent: true,
        contentType: 'application/json',
    })
}

export async function closeAmqp() {
    try { await channel?.close() } catch {}
    try { await conn?.close() } catch {}
}
