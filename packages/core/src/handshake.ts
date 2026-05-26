import {hashWith} from './crypto'
import {genId, mkErr, resetTimer, selfId, toErrorMessage, toHex} from './utils'
import type {
  DataPayload,
  HandshakePayload,
  HandshakeReceiver,
  HandshakeSender,
  JsonValue,
  PeerHandle,
  PeerHandshake
} from './types'
import {Cause, Data, Deferred, Effect, Fiber, Match} from 'effect'

const overlapRoomPasswordErr = mkErr('incorrect password for overlapping room')

export const createPasswordHandshake = (
  password: string,
  appId: string,
  roomId: string
): {
  run: (
    send: HandshakeSender,
    receive: HandshakeReceiver,
    isInitiator: boolean
  ) => Promise<void>
  compose: (userHandshake?: PeerHandshake) => PeerHandshake | undefined
} => {
  const hashChallenge = (challenge: string): Promise<string> =>
    hashWith('SHA-256', `${challenge}:${password}:${appId}:${roomId}`).then(
      toHex
    )

  const run = async (
    send: HandshakeSender,
    receive: HandshakeReceiver,
    isInitiator: boolean
  ): Promise<void> => {
    if (!password) {
      return
    }

    if (isInitiator) {
      const challenge = genId(36)
      await send({__trystero_pw: 'challenge', c: challenge})
      const {data} = await receive()

      if (
        !data ||
        typeof data !== 'object' ||
        (data as {__trystero_pw?: unknown}).__trystero_pw !== 'response' ||
        typeof (data as {h?: unknown}).h !== 'string'
      ) {
        throw overlapRoomPasswordErr
      }

      const expected = await hashChallenge(challenge)

      if ((data as {h: string}).h !== expected) {
        throw overlapRoomPasswordErr
      }

      return
    }

    const {data} = await receive()

    if (
      !data ||
      typeof data !== 'object' ||
      (data as {__trystero_pw?: unknown}).__trystero_pw !== 'challenge' ||
      typeof (data as {c?: unknown}).c !== 'string'
    ) {
      throw overlapRoomPasswordErr
    }

    await send({
      __trystero_pw: 'response',
      h: await hashChallenge((data as {c: string}).c)
    })
  }

  const compose = (userHandshake?: PeerHandshake): PeerHandshake | undefined =>
    password || userHandshake
      ? async (peerId, send, receive, isInitiator): Promise<void> => {
          await run(send, receive, isInitiator)
          await userHandshake?.(peerId, send, receive, isInitiator)
        }
      : undefined

  return {run, compose}
}
export type PeerHandshakeFiber = Fiber.Fiber<
  void,
  Cause.TimeoutError | PeerHandshakeFailed
>
type PendingPeerState = {
  peer: PeerHandle
  isActive: boolean
  didLocalHandshakePass: boolean
  didReceiveRemoteReady: boolean
  handshakeTimer: ReturnType<typeof setTimeout> | null
  pendingHandshakePayloads: HandshakePayload[]
  handshakeWaiters: Array<{
    resolve: (payload: HandshakePayload) => void
    reject: (error: Error) => void
  }>
  isRemoteReady: Deferred.Deferred<true>
  handshakeFiber: PeerHandshakeFiber | null
}

export class PeerHandshakeFailed extends Data.TaggedError('HandshakeFailed')<{
  readonly peerId: string
  readonly peer: PeerHandle
  readonly cause: Error
}> {}

type HandshakeManagerDeps = {
  onPeerHandshake?: PeerHandshake
  onHandshakeError?: (peerId: string, error: string) => void
  handshakeTimeoutMs: number
  sendHandshakeData: (
    data: DataPayload,
    peerId: string,
    metadata?: JsonValue
  ) => Promise<void[]>
  sendHandshakeReady: (data: string, peerId: string) => Promise<void[]>
  onActivate: (peerId: string, peer: PeerHandle) => void
  onFailure: (peerId: string, peer: PeerHandle, reason: Error) => void
}

const toHandshakeErrorMessage = (error: Error): string => {
  const message = toErrorMessage(error, 'unknown error')

  return message.startsWith('handshake ')
    ? message
    : `handshake failed: ${message}`
}

export const createHandshakeManager = ({
  onPeerHandshake,
  onHandshakeError,
  handshakeTimeoutMs,
  sendHandshakeData,
  sendHandshakeReady,
  onActivate,
  onFailure
}: HandshakeManagerDeps): {
  addPeer: (id: string, peer: PeerHandle) => void
  clearPeer: (id: string, error: Error) => void
  canReceiveFromPeer: (id: string, receiveWhilePending: boolean) => boolean
  start: (id: string, peer: PeerHandle) => void
  receiveHandshakeData: (
    data: DataPayload,
    id: string,
    metadata?: JsonValue
  ) => void
  receiveHandshakeReady: (id: string) => void
} => {
  const peerStates: Record<string, PendingPeerState> = {}

  const maybeActivatePeer = (id: string, peer?: PeerHandle): void => {
    const state = peerStates[id]

    if (!state || (peer && state.peer !== peer) || state.isActive) {
      return
    }

    if (!state.didLocalHandshakePass || !state.didReceiveRemoteReady) {
      return
    }

    state.isActive = true
    state.handshakeTimer = resetTimer(state.handshakeTimer)
    onActivate(id, state.peer)
  }

  const failPeerHandshake = (
    id: string,
    peer: PeerHandle,
    reason: Error
  ): void => {
    const state = peerStates[id]

    if (!state || state.peer !== peer) {
      return
    }

    const error = toHandshakeErrorMessage(reason)

    onHandshakeError?.(id, error)
    onFailure(id, peer, mkErr(error))
  }

  // an effect that awaits and properly error tracks operations along the peer handshake chain
  const makePeerHandshakeEffect = Effect.fnUntraced(function* (
    id: string,
    peer: PeerHandle
  ) {
    const state = peerStates[id]

    if (!state || state.peer !== peer) {
      return
    }

    const sendHandshake: HandshakeSender = async (data, metadata) => {
      await sendHandshakeData(data, id, metadata)
    }

    const receiveHandshake: HandshakeReceiver = () =>
      new Promise<HandshakePayload>((resolve, reject) => {
        const current = peerStates[id]

        if (!current || current.peer !== peer) {
          reject(mkErr('peer disconnected during handshake'))
          return
        }

        const payload = current.pendingHandshakePayloads.shift()

        if (payload) {
          resolve(payload)
          return
        }

        current.handshakeWaiters.push({
          resolve,
          reject: error => reject(error)
        })
      })
    // wait for handshake callbacks to resolve
    const isInitiator = selfId < id
    if (onPeerHandshake) {
      yield* Effect.tryPromise({
        try: () =>
          Promise.resolve(
            onPeerHandshake(id, sendHandshake, receiveHandshake, isInitiator)
          ),
        catch: err =>
          new PeerHandshakeFailed({
            peerId: id,
            peer: peer,
            cause: mkErr(
              `handshake process callback rejected with unknown error ${err}`
            )
          })
      })
    }
    // send local ready signal
    yield* Effect.tryPromise({
      try: () => {
        const readyPromise = sendHandshakeReady('', id)
        const cur = peerStates[id]
        if (cur && cur.peer === peer) {
          cur.didLocalHandshakePass = true
        }
        maybeActivatePeer(id, peer) // optimistic check for local machine connections maybe?
        return readyPromise
      },
      catch: err =>
        new PeerHandshakeFailed({
          peerId: id,
          peer: peer,
          cause: mkErr(
            `handshake failed sending local readiness: ${toErrorMessage(
              err,
              'unknown send failure'
            )}`
          )
        })
    })
    // wait for remote ready signal to arrive
    yield* Deferred.await(state.isRemoteReady)
  })

  return {
    addPeer: (id, peer) => {
      peerStates[id] = {
        peer,
        isActive: false,
        didLocalHandshakePass: false,
        didReceiveRemoteReady: false,
        isRemoteReady: Deferred.makeUnsafe<true>(),
        handshakeTimer: null,
        pendingHandshakePayloads: [],
        handshakeWaiters: [],
        handshakeFiber: null
      }
    },

    clearPeer: (id, error) => {
      const state = peerStates[id]

      if (!state) {
        return
      }

      Effect.all([
        Deferred.interrupt(state.isRemoteReady),
        state.handshakeFiber
          ? Fiber.interrupt(state.handshakeFiber)
          : Effect.void
      ]).pipe(Effect.runSync)
      state.handshakeTimer = resetTimer(state.handshakeTimer)
      state.pendingHandshakePayloads.length = 0
      state.handshakeWaiters.splice(0).forEach(waiter => waiter.reject(error))
      delete peerStates[id]
    },

    canReceiveFromPeer: (id, receiveWhilePending) => {
      const state = peerStates[id]

      return Boolean(state && (state.isActive || receiveWhilePending))
    },

    start: (id, peer) => {
      const handshake = makePeerHandshakeEffect(id, peer)
      if (!handshake) {
        return
      }
      peerStates[id]!.handshakeFiber = handshake.pipe(
        Effect.timeout(handshakeTimeoutMs),
        Effect.tapError(err =>
          Match.value(err).pipe(
            Match.tagsExhaustive({
              TimeoutError: _ =>
                Effect.sync(() => {
                  failPeerHandshake(
                    id,
                    peer,
                    mkErr(`handshake timed out after ${handshakeTimeoutMs}ms`)
                  )
                }),
              HandshakeFailed: err =>
                Effect.sync(() => failPeerHandshake(id, peer, err.cause))
            })
          )
        ),
        Effect.tap(() => Effect.sync(() => maybeActivatePeer(id, peer))),
        Effect.runFork
      )
    },

    receiveHandshakeData: (data, id, metadata) => {
      const state = peerStates[id]

      if (!state || state.isActive) {
        return
      }

      const payload =
        metadata === undefined ? {data} : ({data, metadata} as HandshakePayload)
      const pending = state.handshakeWaiters.shift()

      if (pending) {
        pending.resolve(payload)
        return
      }

      state.pendingHandshakePayloads.push(payload)
    },

    receiveHandshakeReady: id => {
      const state = peerStates[id]

      if (!state || state.isActive) {
        return
      }

      state.didReceiveRemoteReady = true
      Deferred.doneUnsafe(state.isRemoteReady, Effect.succeed(true))
      maybeActivatePeer(id)
    }
  }
}
