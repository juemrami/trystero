import {
  Context,
  Data,
  Effect,
  Exit,
  Fiber,
  flow,
  Match,
  pipe,
  PubSub,
  Ref,
  Scope,
  Stream,
  Types
} from 'effect'
import type {PeerHandle} from './types'
import {RoomPeerManager} from './room-peer-manager'
import {ActionWireManager} from './action-wire'
import {HandshakeManager, toHandshakeErrorMessage} from './handshake'
import {mkErr} from './utils'

type BaseRoomPeer = {
  peerId: string
  handle: PeerHandle
  scope: typeof RoomPeerScope.Service
  dataStream: Stream.Stream<ArrayBuffer>
}
export type RoomPeerState = Data.TaggedEnum<{
  Detected: BaseRoomPeer
  Handshaking: BaseRoomPeer & {
    timeoutAt: number
  }
  Active: BaseRoomPeer & {
    activatedAt: number
  }
}>
export const RoomPeerState = Data.taggedEnum<RoomPeerState>()

export type RoomPeerEvent = Data.TaggedEnum<{
  PeerActivated: {
    peerId: string
  }
  PeerDetected: {
    peerId: string
  }
  PeerClosed: {
    peerId: string
    reason: Types.Tags<RoomPeerExitValue>
    wasActive: boolean
  }
}>
export const RoomPeerEvent = Data.taggedEnum<RoomPeerEvent>()

export type RoomPeerExitValue = Data.TaggedEnum<{
  PeerLeftRoom: {
    peerId: string
    peer: PeerHandle
    message: string
  }
  PeerDisconnected: {
    peerId: string
    peer: PeerHandle
    message: string
  }
  HandshakeFailed: {
    peerId: string
    peer: PeerHandle
    message: string
  }
  RoomLeft: {
    message: string
  }
  PeerReplaced: {
    peerId: string
    current: PeerHandle
    next: PeerHandle
    message: string
  }
}>
export const RoomPeerExitValue = Data.taggedEnum<RoomPeerExitValue>()

export class RoomPeerScope extends Context.Service<RoomPeerScope>()(
  'PeerScope',
  {
    make: Effect.gen(function* () {
      const scope = yield* Scope.make()
      const service = {
        internal: scope,
        forkIn: flow(Effect.forkIn(scope)),
        addFinalizerExit: (
          finalizer: (
            exit: Exit.Exit<RoomPeerExitValue, any>
          ) => Effect.Effect<unknown>
        ) => Scope.addFinalizerExit(scope, finalizer),
        close: (exit: Exit.Exit<RoomPeerExitValue, any>) =>
          Scope.close(scope, exit),
        exit: <T extends Types.Tags<RoomPeerExitValue>>(
          reason: T,
          value: Data.TaggedEnum.Args<RoomPeerExitValue, T>
        ) => Scope.close(scope, Exit.succeed({_tag: reason, ...(value as any)}))
      }
      return service
    })
  }
) {}

export const makeRoomPeer = Effect.fn(function* (args: {
  handle: PeerHandle
  peerId: string
  handshakeTimeoutMs: number
  onHandshakeError?: ((peerId: string, error: string) => void) | undefined
}) {
  const roomPeerManager = yield* RoomPeerManager
  const existing = yield* roomPeerManager.get(args.peerId)
  if (existing) {
    if (args.handle !== existing.handle) {
      existing.scope.exit('PeerReplaced', {
        current: existing.handle,
        next: args.handle,
        peerId: args.peerId,
        message: 'peer replaced'
      })
    } else {
      return false
    }
  }

  const peerScope = yield* RoomPeerScope.make
  const dataPubSub = yield* PubSub.bounded<ArrayBuffer>({
    capacity: 128
  })

  args.handle.setHandlers({
    data: data => pipe(PubSub.publish(dataPubSub, data), Effect.runSync),
    close: () =>
      peerScope
        .exit('PeerDisconnected', {
          peerId: args.peerId,
          peer: args.handle,
          message: 'peer disconnected'
        })
        .pipe(Effect.runSync),
    error: err =>
      peerScope
        .exit('PeerDisconnected', {
          peerId: args.peerId,
          peer: args.handle,
          message: 'peer encountered an error.\n' + err.message
        })
        .pipe(Effect.runSync)
  })

  yield* roomPeerManager.events.publish({
    _tag: 'PeerDetected',
    peerId: args.peerId
  })
  const newState = RoomPeerState.Detected({
    ...args,
    scope: peerScope,
    dataStream: Stream.fromPubSub(dataPubSub)
  })
  const peerStateRef = yield* Ref.make<RoomPeerState>(newState)
  // leak: peer has to be registered in room manager because wire action expects to find it there
  yield* roomPeerManager.add(args.peerId, peerStateRef)
  // leak: peer has to be registered in wire action to monitor is scope and end transmissions early
  yield* ActionWireManager.registerPeer(newState)

  // for legacy error messages
  const failPeerHandshake = (
    id: string,
    peer: PeerHandle,
    reason: Error
  ): void => {
    const error = toHandshakeErrorMessage(reason)
    args.onHandshakeError?.(id, error)
  }

  const handshakeManager = yield* HandshakeManager
  const handshakeFiber = yield* Effect.forkIn(
    pipe(
      handshakeManager.handshake(peerStateRef),
      Effect.timeout(args.handshakeTimeoutMs),
      Effect.tap(_ =>
        Effect.all([
          Ref.update(peerStateRef, prev =>
            RoomPeerState.Active({
              ...prev,
              activatedAt: Date.now()
            })
          ),
          roomPeerManager.events.publish(
            RoomPeerEvent.PeerActivated({
              peerId: args.peerId
            })
          )
        ])
      ),
      Effect.tapError(err =>
        Match.value(err).pipe(
          Match.tagsExhaustive({
            TimeoutError: _ =>
              Effect.sync(() => {
                failPeerHandshake(
                  args.peerId,
                  args.handle,
                  mkErr(
                    `handshake timed out after ${args.handshakeTimeoutMs}ms`
                  )
                )
              }),
            HandshakeFailed: err =>
              Effect.sync(() =>
                failPeerHandshake(args.peerId, args.handle, err.cause)
              )
          })
        )
      )
    ),
    peerScope.internal,
    {startImmediately: true}
  )
  yield* Ref.update(peerStateRef, prev =>
    RoomPeerState.Handshaking({
      ...prev,
      timeoutAt: Date.now() + args.handshakeTimeoutMs
    })
  )
  yield* peerScope.addFinalizerExit(exit =>
    Effect.all([
      Fiber.interrupt(handshakeFiber),
      Effect.gen(function* () {
        args.handle.destroy()
        const state = yield* Ref.get(peerStateRef)
        yield* roomPeerManager.events.publish(
          RoomPeerEvent.PeerClosed({
            peerId: args.peerId,
            reason: Exit.isSuccess(exit) ? exit.value._tag : 'PeerDisconnected',
            wasActive: state._tag === 'Active'
          })
        )
      })
    ])
  )
  const roomScope = yield* Effect.scope
  yield* Scope.addFinalizer(
    roomScope,
    peerScope.exit('RoomLeft', {message: 'room left'})
  )
  return true
})
