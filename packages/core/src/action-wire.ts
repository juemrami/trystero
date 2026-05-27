import {
  all,
  alloc,
  decodeBytes,
  encodeBytes,
  fromJson,
  libName,
  mkErr,
  noOp,
  resetTimer,
  toJson
} from './utils'
import type {DataPayload, JsonValue, PeerHandle, TargetPeers} from './types'
import {
  Context,
  Data,
  Effect,
  Fiber,
  flow,
  pipe,
  PubSub,
  Ref,
  Result,
  Scope,
  Stream,
  Types
} from 'effect'
import type {RoomPeerScope, RoomPeerState} from './room-peer'
import {RoomPeerManager} from './room-peer-manager'

const TypedArray = Object.getPrototypeOf(Uint8Array)
const typeByteLimit = 32
const nonceByteLimit = 2
const typeIndex = 0
const nonceIndex = typeIndex + typeByteLimit
const tagIndex = nonceIndex + nonceByteLimit
const progressIndex = tagIndex + 1
const payloadIndex = progressIndex + 1
const chunkSize = 16 * 2 ** 10 - payloadIndex
const oneByteMax = 0xff
const twoByteMax = 0xffff
const buffLowEvent = 'bufferedamountlow'
const channelCloseEvent = 'close'
const channelErrorEvent = 'error'
const backpressureWaitTimeoutMs = 10_000

export const internalActionNs = (ns: string): string => '@_' + ns

export type ActionOptions = {
  sendToPending: boolean
  receiveWhilePending: boolean
}

export type InternalActionSender<T extends DataPayload = DataPayload> = (
  data: T,
  targetPeers?: TargetPeers,
  metadata?: JsonValue,
  progress?: (percent: number, peerId: string, metadata?: JsonValue) => void,
  signal?: AbortSignal
) => Promise<void[]>

export type InternalActionReceiver<T extends DataPayload = DataPayload> = (
  receiver: (data: T, peerId: string, metadata?: JsonValue) => void
) => void

export type InternalActionProgress = (
  progressHandler: (
    percent: number,
    peerId: string,
    metadata?: JsonValue
  ) => void
) => void

export type InternalAction<T extends DataPayload = DataPayload> = {
  send: InternalActionSender<T>
  onMessage: InternalActionReceiver<T>
  onProgress: InternalActionProgress
}

type WireActionState = {
  onComplete: (
    payload: DataPayload,
    peerId: string,
    metadata?: JsonValue
  ) => void
  onProgress: (percent: number, peerId: string, metadata?: JsonValue) => void
  setOnComplete: (
    f: (payload: DataPayload, peerId: string, metadata?: JsonValue) => void
  ) => void
  setOnProgress: (
    f: (percent: number, peerId: string, metadata?: JsonValue) => void
  ) => void
  send: InternalActionSender
  options: ActionOptions
}

type PendingTransmission = {
  chunks: Uint8Array[]
  meta?: JsonValue
}

type PendingActionPayload = {
  payload: DataPayload
  peerId: string
  metadata?: JsonValue
}

type ActionWireManagerDeps = {
  getPeer: (id: string, includePending: boolean) => PeerHandle | undefined
  getPeerIds: (includePending: boolean) => string[]
  canReceiveFromPeer: (id: string, receiveWhilePending: boolean) => boolean
  throwIfAborted: (signal?: AbortSignal) => void
}

const toByteArray = (value: ArrayBuffer | ArrayBufferView): Uint8Array =>
  value instanceof ArrayBuffer
    ? new Uint8Array(value)
    : new Uint8Array(value.buffer, value.byteOffset, value.byteLength)

const waitForBufferedAmountLow = (
  channel: RTCDataChannel,
  timeoutMs = backpressureWaitTimeoutMs
): Promise<boolean> => {
  if (
    channel.readyState !== 'open' ||
    channel.bufferedAmount <= channel.bufferedAmountLowThreshold
  ) {
    return Promise.resolve(channel.readyState === 'open')
  }

  return new Promise<boolean>(res => {
    let settled = false
    let timeout: ReturnType<typeof setTimeout> | null = null

    const finish = (didDrain: boolean): void => {
      if (settled) {
        return
      }

      settled = true
      channel.removeEventListener(buffLowEvent, onBufferLow)
      channel.removeEventListener(channelCloseEvent, onCloseOrError)
      channel.removeEventListener(channelErrorEvent, onCloseOrError)

      resetTimer(timeout)
      res(didDrain)
    }

    const onBufferLow = (): void => finish(true)
    const onCloseOrError = (): void => finish(false)

    channel.addEventListener(buffLowEvent, onBufferLow)
    channel.addEventListener(channelCloseEvent, onCloseOrError)
    channel.addEventListener(channelErrorEvent, onCloseOrError)

    timeout = setTimeout(() => finish(false), timeoutMs)

    if (channel.readyState !== 'open') {
      finish(false)
      return
    }

    if (channel.bufferedAmount <= channel.bufferedAmountLowThreshold) {
      finish(true)
    }
  })
}

type ChannelBufferFailure = Data.TaggedEnum<{
  ChannelClosed: {}
  ChannelError: {error: RTCErrorEvent}
  TimeoutError: {}
  ChannelNotReady: {state: RTCDataChannelState}
}>

// waits for the channel buffered data size to fall below `channel.bufferedAmountLowThreshold`
const waitForChannelBufferSpace = Effect.fnUntraced(function* (
  channel,
  timeoutMs = backpressureWaitTimeoutMs
) {
  const {ChannelClosed, ChannelError, TimeoutError, ChannelNotReady} =
    Data.taggedEnum<ChannelBufferFailure>()
  if (channel.readyState !== 'open') {
    return Result.fail(ChannelNotReady({state: channel.readyState}))
  }
  if (channel.bufferedAmount <= channel.bufferedAmountLowThreshold) {
    return Result.void
  }
  const bufferLowEvents = Stream.fromEventListener(channel, buffLowEvent)
  const closeEvents = Stream.fromEventListener(channel, channelCloseEvent)
  const errorEvents = Stream.fromEventListener(channel, channelErrorEvent)
  const result = Stream.mergeAll<
    Result.Result<void, ChannelBufferFailure>,
    never,
    never
  >(
    [
      bufferLowEvents.pipe(Stream.map(_ => Result.void)),
      closeEvents.pipe(Stream.map(_ => Result.fail(ChannelClosed()))),
      errorEvents.pipe(
        Stream.map(error =>
          Result.fail(ChannelError({error: error as RTCErrorEvent}))
        )
      )
    ],
    {concurrency: 3}
  ).pipe(
    Stream.take(1),
    Stream.runCollect,
    Effect.map(res => res[0]!),
    Effect.timeout(timeoutMs),
    Effect.catchTag('TimeoutError', _ =>
      Effect.succeed(Result.fail(TimeoutError()))
    )
  )
  return yield* result
})

export type ActionWireEvent = Data.TaggedEnum<{
  ReceiveComplete: {
    payload: DataPayload
    peerId: string
    isBinary?: boolean | undefined
    metadata?: JsonValue | undefined
    action: string
  }
  ReceiveInProgress: {
    percent: number
    peerId: string
    metadata?: JsonValue | undefined
    isBinary?: boolean | undefined
    action: string
  }
  SendInProgress: {
    percent: number
    peerId: string
    metadata?: JsonValue | undefined
    action: string
  }
  SendComplete: {
    peerId: string
    metadata?: JsonValue | undefined
    action: string
  }
  ReceiveError: {
    error: ActionReceiveError
  }
}>
export const ActionWireEvent = Data.taggedEnum<ActionWireEvent>()

export type MakeActionFailure = Data.TaggedEnum<{
  RedefinitionAttempted: {}
  EmptyTypeName: {}
  TypeNameTooLong: {byteLength: number; byteLimit: number}
}>
export const MakeActionFailure =Data.taggedEnum<MakeActionFailure>()
const {RedefinitionAttempted, EmptyTypeName, TypeNameTooLong} = MakeActionFailure

export class MakeActionError extends Data.TaggedError('MakeActionError')<{
  readonly reason: MakeActionFailure
  readonly cause: unknown
}> {}
export class ActionSendError extends Data.TaggedError('ActionSendError')<{
  readonly reason:
    | 'PeerNotFound'
    | 'PeerChanged'
    | 'PeerNotReady'
    | 'InvalidActionData'
    | Types.Tags<ChannelBufferFailure>
  readonly cause: Error | RTCErrorEvent
}> {}
export class ActionReceiveError extends Data.TaggedError('ActionReceiveError')<{
  readonly reason:
    | 'PeerNotFound'
    | 'PeerChanged'
    | 'PeerNotReady'
    | 'PeerDisconnected'
    | 'InvalidActionType'
  readonly cause: Error | RTCErrorEvent
}> {}

export type ActionWire<T extends DataPayload = DataPayload> = {
  /** call will synchronously error if invalid data or target peers.
   * Transmission errors will be observable in their respective fibers
   * */
  send: (
    data: T,
    targetPeers: string[],
    metadata?: JsonValue
  ) => Effect.Effect<Array<Fiber.Fiber<true, ActionSendError>>, ActionSendError>
  events: Stream.Stream<ActionWireEvent>
}

export class ActionWireManager extends Context.Service<ActionWireManager>()(
  'ActionWireManager',
  {
    make: Effect.fnUntraced(function* (args?: {
      /** todo: remove. for legacy actionManager */
      onRegisteredPeerData?: (
        peerId: string,
        data: ArrayBuffer
      ) => Effect.Effect<any>
    }) {
      const createdActionsCache: Map<
        string,
        {
          action: ActionWire
          receiver: (
            peerId: string,
            data: ArrayBuffer
          ) => Effect.Effect<any, ActionReceiveError>
          options?: Partial<ActionOptions>
        }
      > = new Map()

      const peerManager = yield* RoomPeerManager
      const registeredPeers = new Map<string, typeof RoomPeerScope.Service>()
      const registeredPeerData = yield* PubSub.unbounded<{
        peerId: string
        peerScope: typeof RoomPeerScope.Service
        data: ArrayBuffer
      }>()
      const pendingTransmissions: Record<
        string,
        Record<string, Record<number, PendingTransmission>>
      > = {}

      const registerPeer: (
        peer: RoomPeerState
      ) => Effect.Effect<Result.Result<void, 'OpenScopeExists'>> =
        Effect.fnUntraced(function* (peer) {
          const existingScope = registeredPeers.get(peer.peerId)
          if (existingScope) {
            if (existingScope.internal.state._tag !== 'Closed') {
              return Result.fail('OpenScopeExists')
            }
          }
          registeredPeers.set(peer.peerId, peer.scope)
          yield* peer.scope.addFinalizerExit(_ =>
            Effect.sync(() => {
              registeredPeers.delete(peer.peerId)
              delete pendingTransmissions[peer.peerId]
            })
          )
          yield* Effect.forkIn(peer.scope.internal)(
            pipe(
              peer.dataStream,
              Stream.tap(data =>
                PubSub.publish(registeredPeerData, {
                  peerId: peer.peerId,
                  peerScope: peer.scope,
                  data
                })
              ),
              Stream.runDrain
            )
          )
          return Result.void
        })

      yield* Effect.addFinalizer(() => PubSub.shutdown(registeredPeerData))

      const makeActionRtcDataChannelSender: (args: {
        sendToPending?: boolean
        type: string
        typeBytesPadded: Uint8Array
        eventHub: PubSub.PubSub<ActionWireEvent>
      }) => ActionWire['send'] = args => {
        let nonce = 0
        return Effect.fnUntraced(function* (data, targets, meta) {
          // throwIfAborted(signal)
          const dataType = typeof data

          if (dataType === 'undefined') {
            return yield* new ActionSendError({
              reason: 'InvalidActionData',
              cause: mkErr('action data cannot be undefined')
            })
          }
          const managedPeers = yield* peerManager.getAll()
          const targetPeers = managedPeers.filter(
            s =>
              (args?.sendToPending ? true : s._tag === 'Active') &&
              targets.includes(s.peerId)
          )
          if (targetPeers.length === 0) {
            return yield* new ActionSendError({
              reason: 'PeerNotFound',
              cause: mkErr(
                `no valid target peers found for action "${args.type}". sendToPending=${Boolean(args?.sendToPending)}`
              )
            })
          }
          const isJson = dataType !== 'string'
          const isBlob = data instanceof Blob
          const isBinary =
            isBlob || data instanceof ArrayBuffer || data instanceof TypedArray
          const hasMeta = meta !== undefined

          const buffer = isBinary
            ? toByteArray(
                isBlob
                  ? yield* Effect.promise(() => data.arrayBuffer())
                  : (data as ArrayBuffer | ArrayBufferView)
              )
            : encodeBytes(isJson ? toJson(data) : (data as string))

          const metaEncoded = hasMeta ? encodeBytes(toJson(meta)) : null

          const chunkTotal =
            Math.ceil(buffer.byteLength / chunkSize) + (hasMeta ? 1 : 0) || 1

          const chunks = alloc(chunkTotal, (_, i) => {
            const isLast = i === chunkTotal - 1
            const isMeta = Boolean(hasMeta && i === 0)
            const chunk = new Uint8Array(
              payloadIndex +
                (isMeta
                  ? (metaEncoded?.byteLength ?? 0)
                  : isLast
                    ? buffer.byteLength -
                      chunkSize * (chunkTotal - (hasMeta ? 2 : 1))
                    : chunkSize)
            )

            chunk.set(args.typeBytesPadded)
            chunk.set([nonce >> 8, nonce & oneByteMax], nonceIndex)
            chunk.set(
              [
                Number(isLast) |
                  (Number(isMeta) << 1) |
                  (Number(isBinary) << 2) |
                  (Number(isJson) << 3)
              ],
              tagIndex
            )
            chunk.set(
              [Math.round(((i + 1) / chunkTotal) * oneByteMax)],
              progressIndex
            )
            chunk.set(
              hasMeta
                ? isMeta
                  ? (metaEncoded ?? new Uint8Array())
                  : buffer.subarray((i - 1) * chunkSize, i * chunkSize)
                : buffer.subarray(i * chunkSize, (i + 1) * chunkSize),
              payloadIndex
            )

            return chunk
          })

          nonce = (nonce + 1) & twoByteMax

          const transmit = Effect.fnUntraced(function* (target: string) {
            const peerRef = peerManager.getRef(target)
            if (!peerRef) {
              return yield* new ActionSendError({
                reason: 'PeerNotFound',
                cause: mkErr(`no peer with id ${target} found`)
              })
            }
            const peer = yield* Ref.get(peerRef)
            if (!args?.sendToPending && peer._tag !== 'Active') {
              return yield* new ActionSendError({
                reason: 'PeerNotReady',
                cause: mkErr(`peer with id ${target} pending handshake`)
              })
            }
            const {channel} = peer.handle
            let chunkN = 0

            while (chunkN < chunkTotal) {
              // throwIfAborted(signal)
              const chunk = chunks[chunkN]

              if (!chunk) {
                break
              }

              if (
                channel &&
                channel.bufferedAmount > channel.bufferedAmountLowThreshold
              ) {
                const result = yield* waitForChannelBufferSpace(channel)

                // throwIfAborted(signal)

                if (Result.isFailure(result)) {
                  return yield* new ActionSendError({
                    reason: result.failure._tag,
                    cause:
                      result.failure._tag === 'ChannelError'
                        ? result.failure.error
                        : mkErr(`failed waiting for data channel buffer`)
                  })
                }
              }

              const currentPeer = yield* peerManager.get(target)

              if (!currentPeer || currentPeer.handle !== peer.handle) {
                return yield* new ActionSendError({
                  reason: 'PeerChanged',
                  cause: mkErr(
                    `target peer handle changed during data transmission`
                  )
                })
              }

              peer.handle.sendData(chunk)
              chunkN++
              const progressByte = chunk[progressIndex] ?? oneByteMax

              yield* PubSub.publish(
                args.eventHub,
                ActionWireEvent.SendInProgress({
                  peerId: target,
                  percent: progressByte / oneByteMax,
                  metadata: meta,
                  action: args.type
                })
              )
            }
            yield* PubSub.publish(
              args.eventHub,
              ActionWireEvent.SendComplete({
                peerId: target,
                metadata: meta,
                action: args.type
              })
            )
            return true as const
          })
          return yield* pipe(
            targetPeers.map(({peerId, scope}) =>
              pipe(transmit(peerId), Effect.forkIn(scope.internal))
            ),
            Effect.all
          )
        })
      }
      // create action specific receiver for incoming data from rtc peers
      const makeActionRtcDataChannelReceiver: (config: {
        type: string
        eventHub: PubSub.PubSub<ActionWireEvent>
      }) => (
        peerId: string,
        data: ArrayBuffer
      ) => Effect.Effect<any, ActionReceiveError> = config =>
        Effect.fnUntraced(function* (peerId: string, data: ArrayBuffer) {
          const buffer = new Uint8Array(data)
          const type = decodeBytes(
            buffer.subarray(typeIndex, nonceIndex)
          ).replaceAll('\x00', '')
          const action = createdActionsCache.get(type)

          if (!action) {
            return yield* new ActionReceiveError({
              reason: 'InvalidActionType',
              cause: mkErr(
                `received data for unregistered action of type="${type}"`
              )
            })
          }

          if (type !== config.type) {
            return yield* new ActionReceiveError({
              reason: 'InvalidActionType',
              cause: mkErr(
                `received data for action of type="${type}" in handler for type="${config.type}"`
              )
            })
          }

          const peer = yield* peerManager.get(peerId)
          if (!peer) {
            return yield* new ActionReceiveError({
              reason: 'PeerNotFound',
              cause: mkErr(`received data from unknown peer ${peerId}`)
            })
          }
          if (
            Boolean(action.options?.receiveWhilePending) === false &&
            peer._tag !== 'Active'
          ) {
            return yield* new ActionReceiveError({
              reason: 'PeerNotReady',
              cause: mkErr(
                `received data from peer ${peerId} who has not completed handshake yet`
              )
            })
          }
          yield* PubSub.publish(config.eventHub, {
            _tag: 'ReceiveInProgress',
            peerId,
            percent: 0,
            metadata: undefined,
            action: type
          })

          const nonce =
            ((buffer[nonceIndex] ?? 0) << 8) | (buffer[nonceIndex + 1] ?? 0)
          const tag = buffer[tagIndex] ?? 0
          const progress = buffer[progressIndex] ?? 0
          const payload = buffer.subarray(payloadIndex)
          const isLast = Boolean(tag & 1)
          const isMeta = Boolean(tag & (1 << 1))
          const isBinary = Boolean(tag & (1 << 2))
          const isJson = Boolean(tag & (1 << 3))

          pendingTransmissions[peerId] ??= {}
          pendingTransmissions[peerId][type] ??= {}

          const target = (pendingTransmissions[peerId][type][nonce] ??= {
            chunks: []
          })

          if (isMeta) {
            target.meta = fromJson<JsonValue>(decodeBytes(payload))
          } else {
            target.chunks.push(payload)
          }
          if (!isLast) {
            return yield* PubSub.publish(
              config.eventHub,
              ActionWireEvent.ReceiveInProgress({
                peerId,
                percent: progress / oneByteMax,
                metadata: target.meta,
                action: type
              })
            )
          }
          const full = new Uint8Array(
            target.chunks.reduce(
              (a: number, c: Uint8Array) => a + c.byteLength,
              0
            )
          )

          target.chunks.reduce((a: number, c: Uint8Array) => {
            full.set(c, a)
            return a + c.byteLength
          }, 0)

          delete pendingTransmissions[peerId][type][nonce]

          const payloadValue = isBinary
            ? full
            : isJson
              ? fromJson<JsonValue>(decodeBytes(full))
              : decodeBytes(full)

          return yield* PubSub.publish(
            config.eventHub,
            ActionWireEvent.ReceiveComplete({
              peerId,
              isBinary,
              metadata: target.meta,
              action: type,
              payload: payloadValue
            })
          )
        })

      // map incoming rtc peer data to the correct action receiver
      yield* Effect.forkScoped(
        pipe(
          Stream.fromPubSub(registeredPeerData),
          Stream.tap(({peerId, data}) =>
            Effect.gen(function* () {
              const buffer = new Uint8Array(data)

              const incomingActionId = decodeBytes(
                buffer.subarray(typeIndex, nonceIndex)
              ).replaceAll('\x00', '')

              const action = createdActionsCache.get(incomingActionId)

              // If effect action handler not found, fall through to legacy handler
              if (action) {
                yield* action.receiver(peerId, data)
              } else if (args?.onRegisteredPeerData) {
                yield* args.onRegisteredPeerData!(peerId, data)
              }
            })
          ),
          Stream.runDrain
        )
      )

      const makeActionWire: <T extends DataPayload = DataPayload>(
        type: string,
        options?: Partial<ActionOptions> | undefined
      ) => Effect.Effect<ActionWire<T>, MakeActionError, Scope.Scope> =
        Effect.fnUntraced(function* (type, options) {
          if (type.length === 0) {
            return yield* new MakeActionError({
              reason: EmptyTypeName(),
              // action type argument is required // old msg
              cause: mkErr(`action type cannot be an empty string`)
            })
          }
          const existing = createdActionsCache.get(type)
          if (existing) {
            const cachedOptions = existing.options
            if (
              Boolean(cachedOptions?.sendToPending) !==
                Boolean(options?.sendToPending) ||
              Boolean(cachedOptions?.receiveWhilePending) !==
                Boolean(options?.receiveWhilePending)
            ) {
              return yield* new MakeActionError({
                reason: RedefinitionAttempted(),
                cause: mkErr(`action type "${type}" cannot be redefined`)
              })
            }
            return existing.action
          }

          const typeBytes = encodeBytes(type)
          if (typeBytes.byteLength > typeByteLimit) {
            return yield* new MakeActionError({
              reason: TypeNameTooLong({
                byteLength: typeBytes.byteLength,
                byteLimit: typeByteLimit
              }),
              cause: mkErr(
                `action type string "${type}" (${typeBytes.byteLength}b) exceeds ` +
                  `byte limit (${typeByteLimit}). Hint: choose a shorter name.`
              )
            })
          }

          const actionEventsHub = yield* PubSub.bounded<ActionWireEvent>({
            capacity: 64
          })

          const normalizedOptions = {
            sendToPending: Boolean(options?.sendToPending),
            receiveWhilePending: Boolean(options?.receiveWhilePending)
          }
          const actionReceiver = flow(
            makeActionRtcDataChannelReceiver({
              type,
              eventHub: actionEventsHub
            }),
            Effect.catch(error =>
              PubSub.publish(
                actionEventsHub,
                ActionWireEvent.ReceiveError({error})
              )
            )
          )
          yield* Effect.addFinalizer(() =>
            Effect.all([
              PubSub.shutdown(actionEventsHub),
              Effect.sync(() => createdActionsCache.delete(type))
            ])
          )

          const actionSurface = {
            send: makeActionRtcDataChannelSender({
              type,
              typeBytesPadded: pipe(new Uint8Array(typeByteLimit), buf => {
                buf.set(typeBytes)
                return buf
              }),
              eventHub: actionEventsHub,
              sendToPending: normalizedOptions.sendToPending
            }),
            events: Stream.fromPubSub(actionEventsHub)
          }
          createdActionsCache.set(type, {
            options: normalizedOptions,
            action: actionSurface,
            receiver: actionReceiver
          })

          return actionSurface
        })
      return {
        makeActionWire,
        registerPeer
      }
    })
  }
) {
  static registerPeer = (
    ...args: Parameters<(typeof this.Service)['registerPeer']>
  ) => this.use(s => s.registerPeer(...args))
}

export const createActionWireManager = ({
  getPeer,
  getPeerIds,
  canReceiveFromPeer,
  throwIfAborted
}: ActionWireManagerDeps): {
  makeInternalAction: <T extends DataPayload = DataPayload>(
    type: string,
    options?: Partial<ActionOptions>
  ) => InternalAction<T>
  handleData: (id: string, data: ArrayBuffer) => void
  clearPeer: (id: string) => void
} => {
  const actions: Record<string, WireActionState> = {}
  const actionsCache: Record<string, InternalAction> = {}
  const pendingTransmissions: Record<
    string,
    Record<string, Record<number, PendingTransmission>>
  > = {}
  const pendingActionPayloads: Record<string, PendingActionPayload[]> = {}
  const iterate = (
    targets: TargetPeers,
    f: (id: string, peer: PeerHandle) => Promise<void> | void,
    {includePending = false}: {includePending?: boolean} = {}
  ): Promise<void>[] =>
    (targets
      ? Array.isArray(targets)
        ? targets
        : [targets]
      : getPeerIds(includePending)
    ).flatMap(id => {
      const peer = getPeer(id, includePending)

      if (!peer) {
        console.warn(`${libName}: no peer with id ${id} found`)
        return []
      }

      return [Promise.resolve(f(id, peer))]
    })

  const makeInternalAction = <T extends DataPayload = DataPayload>(
    type: string,
    options: Partial<ActionOptions> = {}
  ): InternalAction<T> => {
    const cached = actionsCache[type]

    if (actions[type] && cached) {
      const cachedOptions = actions[type].options

      if (
        cachedOptions.sendToPending !== Boolean(options.sendToPending) ||
        cachedOptions.receiveWhilePending !==
          Boolean(options.receiveWhilePending)
      ) {
        throw mkErr(`action type "${type}" cannot be redefined`)
      }

      return cached as unknown as InternalAction<T>
    }

    if (!type) {
      throw mkErr('action type argument is required')
    }

    const typeBytes = encodeBytes(type)

    if (typeBytes.byteLength > typeByteLimit) {
      throw mkErr(
        `action type string "${type}" (${typeBytes.byteLength}b) exceeds ` +
          `byte limit (${typeByteLimit}). Hint: choose a shorter name.`
      )
    }

    const normalizedOptions = {
      sendToPending: Boolean(options.sendToPending),
      receiveWhilePending: Boolean(options.receiveWhilePending)
    }
    const typeBytesPadded = new Uint8Array(typeByteLimit)
    typeBytesPadded.set(typeBytes)

    let nonce = 0

    actions[type] = {
      onComplete: noOp as (
        payload: DataPayload,
        peerId: string,
        metadata?: JsonValue
      ) => void,
      onProgress: noOp as (
        percent: number,
        peerId: string,
        metadata?: JsonValue
      ) => void,

      setOnComplete: f => {
        actions[type]!.onComplete = f

        const pending = pendingActionPayloads[type]

        if (pending?.length) {
          delete pendingActionPayloads[type]
          pending.forEach(({payload, peerId, metadata}) =>
            f(payload, peerId, metadata)
          )
        }
      },

      setOnProgress: f => {
        actions[type]!.onProgress = f
      },

      send: async (data, targets, meta, onProgress, signal) => {
        throwIfAborted(signal)

        const dataType = typeof data

        if (dataType === 'undefined') {
          throw mkErr('action data cannot be undefined')
        }

        const isJson = dataType !== 'string'
        const isBlob = data instanceof Blob
        const isBinary =
          isBlob || data instanceof ArrayBuffer || data instanceof TypedArray
        const hasMeta = meta !== undefined

        const buffer = isBinary
          ? toByteArray(
              isBlob
                ? await data.arrayBuffer()
                : (data as ArrayBuffer | ArrayBufferView)
            )
          : encodeBytes(isJson ? toJson(data) : (data as string))

        const metaEncoded = hasMeta ? encodeBytes(toJson(meta)) : null

        const chunkTotal =
          Math.ceil(buffer.byteLength / chunkSize) + (hasMeta ? 1 : 0) || 1

        const chunks = alloc(chunkTotal, (_, i) => {
          const isLast = i === chunkTotal - 1
          const isMeta = Boolean(hasMeta && i === 0)
          const chunk = new Uint8Array(
            payloadIndex +
              (isMeta
                ? (metaEncoded?.byteLength ?? 0)
                : isLast
                  ? buffer.byteLength -
                    chunkSize * (chunkTotal - (hasMeta ? 2 : 1))
                  : chunkSize)
          )

          chunk.set(typeBytesPadded)
          chunk.set([nonce >> 8, nonce & oneByteMax], nonceIndex)
          chunk.set(
            [
              Number(isLast) |
                (Number(isMeta) << 1) |
                (Number(isBinary) << 2) |
                (Number(isJson) << 3)
            ],
            tagIndex
          )
          chunk.set(
            [Math.round(((i + 1) / chunkTotal) * oneByteMax)],
            progressIndex
          )
          chunk.set(
            hasMeta
              ? isMeta
                ? (metaEncoded ?? new Uint8Array())
                : buffer.subarray((i - 1) * chunkSize, i * chunkSize)
              : buffer.subarray(i * chunkSize, (i + 1) * chunkSize),
            payloadIndex
          )

          return chunk
        })

        nonce = (nonce + 1) & twoByteMax

        await all(
          iterate(
            targets,
            async (id, peer) => {
              const {channel} = peer
              let chunkN = 0

              while (chunkN < chunkTotal) {
                throwIfAborted(signal)

                const chunk = chunks[chunkN]

                if (!chunk) {
                  break
                }

                if (
                  channel &&
                  channel.bufferedAmount > channel.bufferedAmountLowThreshold
                ) {
                  const didDrain = await waitForBufferedAmountLow(channel)

                  throwIfAborted(signal)

                  if (!didDrain) {
                    break
                  }
                }

                const currentPeer = getPeer(id, normalizedOptions.sendToPending)

                if (!currentPeer || currentPeer !== peer) {
                  break
                }

                peer.sendData(chunk)
                chunkN++
                const progressByte = chunk[progressIndex] ?? oneByteMax
                onProgress?.(progressByte / oneByteMax, id, meta)
              }
            },
            {includePending: normalizedOptions.sendToPending}
          )
        )

        return []
      },

      options: normalizedOptions
    }

    return (actionsCache[type] = {
      send: actions[type].send as InternalActionSender,
      onMessage: actions[type].setOnComplete as InternalActionReceiver,
      onProgress: actions[type].setOnProgress as InternalActionProgress
    }) as unknown as InternalAction<T>
  }

  const handleData = (id: string, data: ArrayBuffer): void => {
    const buffer = new Uint8Array(data)
    const type = decodeBytes(buffer.subarray(typeIndex, nonceIndex)).replaceAll(
      '\x00',
      ''
    )
    const action = actions[type]

    if (!canReceiveFromPeer(id, Boolean(action?.options.receiveWhilePending))) {
      return
    }

    const nonce =
      ((buffer[nonceIndex] ?? 0) << 8) | (buffer[nonceIndex + 1] ?? 0)
    const tag = buffer[tagIndex] ?? 0
    const progress = buffer[progressIndex] ?? 0
    const payload = buffer.subarray(payloadIndex)
    const isLast = Boolean(tag & 1)
    const isMeta = Boolean(tag & (1 << 1))
    const isBinary = Boolean(tag & (1 << 2))
    const isJson = Boolean(tag & (1 << 3))

    pendingTransmissions[id] ??= {}
    pendingTransmissions[id][type] ??= {}

    const target = (pendingTransmissions[id][type][nonce] ??= {chunks: []})

    if (isMeta) {
      target.meta = fromJson<JsonValue>(decodeBytes(payload))
    } else {
      target.chunks.push(payload)
    }

    action?.onProgress(progress / oneByteMax, id, target.meta)

    if (!isLast) {
      return
    }

    const full = new Uint8Array(
      target.chunks.reduce((a: number, c: Uint8Array) => a + c.byteLength, 0)
    )

    target.chunks.reduce((a: number, c: Uint8Array) => {
      full.set(c, a)
      return a + c.byteLength
    }, 0)

    delete pendingTransmissions[id][type][nonce]

    const payloadValue = isBinary
      ? full
      : isJson
        ? fromJson<JsonValue>(decodeBytes(full))
        : decodeBytes(full)

    if (action) {
      action.onComplete(payloadValue, id, target.meta)
      return
    }

    ;(pendingActionPayloads[type] ??= []).push({
      payload: payloadValue,
      peerId: id,
      ...(target.meta === undefined ? {} : {metadata: target.meta})
    })
  }

  return {
    makeInternalAction,
    handleData,
    clearPeer: id => {
      delete pendingTransmissions[id]
    }
  }
}
