import {hashWith} from './crypto'
import {genId, mkErr, selfId, toErrorMessage, toHex} from './utils'
import type {
  HandshakePayload,
  HandshakeReceiver,
  HandshakeSender,
  PeerHandle,
  PeerHandshake
} from './types'
import {
  Cause,
  Context,
  Data,
  Deferred,
  Effect,
  Fiber,
  pipe,
  Ref,
  Result,
  Stream
} from 'effect'
import {
  ActionWireManager,
  ActionSendError,
  ActionWireEvent,
  internalActionNs
} from './action-wire'
import type {RoomPeerState} from './room-peer'

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

export class PeerHandshakeError extends Data.TaggedError('HandshakeFailed')<{
  readonly reason?: ActionSendError
  readonly peerId: string
  readonly peer: PeerHandle
  readonly cause: Error //todo refine error types
}> {}

export const toHandshakeErrorMessage = (error: Error): string => {
  const message = toErrorMessage(error, 'unknown error')

  return message.startsWith('handshake ')
    ? message
    : `handshake failed: ${message}`
}
export class HandshakeManager extends Context.Service<HandshakeManager>()(
  'HandshakeManager',
  {
    make: Effect.fnUntraced(function* ({
      onPeerHandshake
    }: {
      /** peer handshake predicate callback */
      onPeerHandshake?: PeerHandshake | undefined
    }) {
      const {makeActionWire} = yield* ActionWireManager
      const dataWire = yield* makeActionWire(internalActionNs('hsdata'), {
        sendToPending: true,
        receiveWhilePending: true
      }).pipe(Effect.catchTag('MakeActionError', Effect.die))
      const readyWire = yield* makeActionWire(internalActionNs('hsready'), {
        sendToPending: true,
        receiveWhilePending: true
      }).pipe(Effect.catchTag('MakeActionError', Effect.die))

      type HandshakeInfo = {
        peer: Ref.Ref<RoomPeerState>
        pendingHandshakePayloads: HandshakePayload[]
        handshakeWaiters: Array<{
          resolve: (payload: HandshakePayload) => void
          reject: (error: Error) => void
        }>
        onComplete: Deferred.Deferred<
          Result.Result<void, PeerHandshakeError | Cause.TimeoutError>
        >
      }

      return {
        handshake: Effect.fnUntraced(function* (
          peerRef: Ref.Ref<RoomPeerState>
        ) {
          const {peerId, handle} = yield* Ref.get(peerRef)
          const isInitiator = selfId < peerId

          const handshakeInfo: HandshakeInfo = {
            peer: peerRef,
            pendingHandshakePayloads: [],
            handshakeWaiters: [],
            onComplete: yield* Deferred.make<any>()
          }

          const sendHandshakePredicate: HandshakeSender = async (
            data,
            metadata
          ) => {
            await dataWire
              .send(data, [peerId], metadata)
              .pipe(Effect.runPromise)
          }
          const receiveHandshakePredicate: HandshakeReceiver = () =>
            new Promise<HandshakePayload>((resolve, reject) => {
              const current = Ref.getUnsafe(peerRef)
              if (current.scope.internal.state._tag === 'Closed') {
                reject(mkErr('peer disconnected during handshake'))
                return
              }
              if (Deferred.isDoneUnsafe(handshakeInfo.onComplete)) {
                reject(mkErr('handshake already exited'))
                return
              }
              const payload = handshakeInfo.pendingHandshakePayloads.shift()
              if (payload) {
                resolve(payload)
                return
              }
              handshakeInfo.handshakeWaiters.push({
                resolve,
                reject: error => reject(error)
              })
            })
          const awaitHandshakePredicates = (onPeerHandshake: PeerHandshake) =>
            Effect.tryPromise({
              try: () =>
                Promise.resolve(
                  onPeerHandshake(
                    peerId,
                    sendHandshakePredicate,
                    receiveHandshakePredicate,
                    isInitiator
                  )
                ),
              catch: err =>
                new PeerHandshakeError({
                  peerId: peerId,
                  peer: handle,
                  cause: mkErr(
                    `handshake predicate process rejected with unknown error ${err}`
                  )
                })
            })
          const sendLocalReady = readyWire.send('', [peerId]).pipe(
            Effect.mapError(
              err =>
                new PeerHandshakeError({
                  peerId: peerId,
                  peer: handle,
                  cause: mkErr(
                    `handshake failed sending local readiness: ${toErrorMessage(
                      err,
                      'unknown send failure'
                    )}`
                  ),
                  reason: err
                })
            ),
            Effect.andThen(sendFibers => Fiber.await(sendFibers[0]!))
          )
          const waitForRemoteReady = Stream.runDrain(
            readyWire.events.pipe(
              Stream.filter(
                event =>
                  ActionWireEvent.$is('ReceiveComplete')(event) &&
                  event.peerId === peerId
              ),
              Stream.take(1)
            )
          )
          const handshake = Effect.gen(function* () {
            if (onPeerHandshake) {
              yield* Effect.scoped(
                Effect.gen(function* () {
                  // listen for incoming handshake data
                  yield* Effect.forkScoped(
                    pipe(
                      dataWire.events,
                      Stream.filter(ActionWireEvent.$is('ReceiveComplete')),
                      Stream.tap(({payload, metadata}) =>
                        Effect.sync(() => {
                          const hsPayload =
                            metadata === undefined
                              ? {data: payload}
                              : {
                                  data: payload,
                                  metadata: metadata
                                }

                          const pending = handshakeInfo.handshakeWaiters.shift()
                          if (pending) {
                            pending.resolve(hsPayload)
                            return Result.void
                          }

                          handshakeInfo.pendingHandshakePayloads.push(hsPayload)
                          return Result.void
                        })
                      ),
                      Stream.runDrain
                    ),
                    {startImmediately: true}
                  )

                  yield* Effect.all(
                    [
                      // do handshake predicate callbacks
                      awaitHandshakePredicates(onPeerHandshake).pipe(
                        // then send ready local signal
                        Effect.andThen(() => sendLocalReady)
                      ),
                      waitForRemoteReady // wait for remote ready
                    ],
                    // listen for remote hsready concurrently
                    // avoids race if it's sent while we're still completing the handshake predicate callbacks
                    {concurrency: 2}
                  )
                })
              )
            } else {
              yield* Effect.all([sendLocalReady, waitForRemoteReady], {
                concurrency: 2
              })
            }
          })
          return yield* handshake
        })
      }
    })
  }
) {}
