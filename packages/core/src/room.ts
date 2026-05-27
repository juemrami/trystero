import {
  entries,
  fromEntries,
  isBrowser,
  keys,
  libName,
  mkErr,
  noOp,
  toError
} from './utils'
import {createActionManager} from './actions'
import {HandshakeManager} from './handshake'
import {createMediaManager, type InternalMediaMeta} from './media'
import type {
  AddMediaOptions,
  DataPayload,
  PeerHandle,
  PeerHandshake,
  Room,
  SharedMediaPeer,
  TargetPeers
} from './types'
import {Context, Effect, Exit, flow, pipe, Scope, Stream} from 'effect'
import {ActionWireManager} from './action-wire'
import {RoomPeerManager} from './room-peer-manager'
import {makeRoomPeer, RoomPeerExitValue, RoomPeerEvent} from './room-peer'

const unloadEvent = 'beforeunload'
const defaultHandshakeTimeoutMs = 10_000
const internalNs = (ns: string): string => '@_' + ns
const beforeUnloadRoomCleanups = new Set<() => void>()

const cleanupActiveRoomsOnBeforeUnload = (): void =>
  beforeUnloadRoomCleanups.forEach(cleanup => cleanup())

const registerBeforeUnloadCleanup = (cleanup: () => void): (() => void) => {
  beforeUnloadRoomCleanups.add(cleanup)

  if (beforeUnloadRoomCleanups.size === 1) {
    addEventListener(unloadEvent, cleanupActiveRoomsOnBeforeUnload)
  }

  return (): void => {
    beforeUnloadRoomCleanups.delete(cleanup)

    if (!beforeUnloadRoomCleanups.size) {
      removeEventListener(unloadEvent, cleanupActiveRoomsOnBeforeUnload)
    }
  }
}

type RoomOptions = {
  onPeerHandshake?: PeerHandshake
  onHandshakeError?: (peerId: string, error: string) => void
  handshakeTimeoutMs?: number
  isPassive?: boolean
}

type PendingPongWaiter = {
  resolve: () => void
  reject: (error: Error) => void
}

const make = Effect.fnUntraced(function* (
  onPeer: (f: (peer: PeerHandle, id: string) => void) => void,
  onPeerLeave: (id: string) => void,
  onSelfLeave: () => void,
  {
    onPeerHandshake,
    onHandshakeError,
    handshakeTimeoutMs = defaultHandshakeTimeoutMs,
    isPassive = false
  }: RoomOptions = {}
) {
  const roomScope = yield* Scope.make()
  const peerMap: Record<string, PeerHandle> = {}
  const activePeerMap: Record<string, PeerHandle> = {}
  const pendingPongs: Record<string, PendingPongWaiter[] | undefined> = {}
  const listeners = {
    onPeerJoin: null as ((peerId: string) => void) | null,
    onPeerLeave: null as ((peerId: string) => void) | null
  }
  let unregisterBeforeUnloadCleanup: () => void = noOp

  const iterate = (
    targets: TargetPeers,
    f: (id: string, peer: PeerHandle) => Promise<void> | void,
    {includePending = false}: {includePending?: boolean} = {}
  ): Promise<void>[] =>
    (targets
      ? Array.isArray(targets)
        ? targets
        : [targets]
      : keys(includePending ? peerMap : activePeerMap)
    ).flatMap(id => {
      const peer = includePending ? peerMap[id] : activePeerMap[id]

      if (!peer) {
        console.warn(`${libName}: no peer with id ${id} found`)
        return []
      }

      return [Promise.resolve(f(id, peer))]
    })

  const mediaManager = createMediaManager({
    iterate: (targets, f) =>
      iterate(targets, (id, peer) => f(id, peer as SharedMediaPeer)),
    isActive: id => Boolean(activePeerMap[id]),
    getSharedMediaPeer: id =>
      (peerMap[id] as SharedMediaPeer | undefined) ?? null
  })

  const RoomScopeCtx = Context.make(Scope.Scope, roomScope)

  const peerManager = yield* RoomPeerManager.make().pipe(
    Effect.provideContext(RoomScopeCtx)
  )
  const PeerManagerCtx = Context.add(RoomScopeCtx, RoomPeerManager, peerManager)

  const actionManager = createActionManager({
    getPeer: (id, includePending) =>
      (includePending ? peerMap : activePeerMap)[id],
    getPeerIds: includePending =>
      keys(includePending ? peerMap : activePeerMap),
    canReceiveFromPeer: Effect.fn(function* (id, receiveWhilePending) {
      const peer = yield* peerManager.get(id)
      if (!peer) {
        return false
      }
      return receiveWhilePending ? true : peer._tag === 'Active'
    }, Effect.runSync)
  })

  const wireManager = yield* ActionWireManager.make({
    onRegisteredPeerData: (...args) =>
      Effect.sync(() => actionManager.handleData(...args))
  }).pipe(Effect.provideContext(PeerManagerCtx))

  const ActionManagerCtx = Context.add(
    PeerManagerCtx,
    ActionWireManager,
    wireManager
  )

  const handshakeManager = yield* HandshakeManager.make({
    onPeerHandshake
  }).pipe(Effect.provideContext(ActionManagerCtx))

  const RoomPeerContext = Context.add(
    ActionManagerCtx,
    HandshakeManager,
    handshakeManager
  )

  const makeActionInternal = actionManager.makeInternalAction

  // Track room peer connectivity events
  yield* Effect.forkIn(roomScope)(
    pipe(
      peerManager.events.stream,
      Stream.tap(
        RoomPeerEvent.$match({
          PeerDetected: () => Effect.void,
          PeerActivated: ({peerId}) =>
            Effect.gen(function* () {
              const peer = yield* peerManager.get(peerId)
              activePeerMap[peerId] = peer!.handle
              listeners.onPeerJoin?.(peerId)
            }),
          PeerClosed: ({reason, peerId, wasActive}) =>
            Effect.sync(() => {
              delete activePeerMap[peerId]
              actionManager.clearPeer(
                peerId,
                mkErr('peer disconnected: ' + reason)
              )
              pendingPongs[peerId]
                ?.splice(0)
                .forEach(waiter => waiter.reject(mkErr('peer disconnected')))
              delete pendingPongs[peerId]
              mediaManager.clearPeer(peerId)

              // keeping announce on HandshakeFailed to match original source behavior
              const announceLeave =
                reason === 'PeerDisconnected' ||
                reason === 'PeerLeftRoom' ||
                reason === 'HandshakeFailed'
              if (announceLeave) {
                onPeerLeave(peerId)
                if (wasActive) {
                  listeners.onPeerLeave?.(peerId)
                }
              }
            })
        })
      ),
      Stream.runDrain
    )
  )

  const leave = async (): Promise<void> => {
    await leaveAction.send('')
    await new Promise<void>(res => setTimeout(res, 99))
    Scope.close(
      roomScope,
      Exit.succeed(RoomPeerExitValue.RoomLeft({message: 'room left'}))
    ).pipe(Effect.runSync)

    unregisterBeforeUnloadCleanup()
    onSelfLeave()
  }

  const pingAction = makeActionInternal<string>(internalNs('ping'))
  const pongAction = makeActionInternal<string>(internalNs('pong'))
  const signalAction = makeActionInternal(internalNs('signal'))
  const streamMetaAction = makeActionInternal<InternalMediaMeta>(
    internalNs('stream')
  )
  const trackMetaAction = makeActionInternal<InternalMediaMeta>(
    internalNs('track')
  )
  const leaveAction = makeActionInternal<string>(internalNs('leave'), {
    sendToPending: true,
    receiveWhilePending: true
  })
  pingAction.onMessage((_, id) => pongAction.send('', id))

  pongAction.onMessage((_, id) => {
    const queue = pendingPongs[id]
    const waiter = queue?.shift()

    waiter?.resolve()

    if (queue && !queue.length) {
      delete pendingPongs[id]
    }
  })

  signalAction.onMessage((sdp, id) => {
    if (!activePeerMap[id]) {
      return
    }

    void peerMap[id]?.signal(sdp as never)
  })

  streamMetaAction.onMessage((meta, id) =>
    mediaManager.receiveStreamMeta(meta, id)
  )

  trackMetaAction.onMessage((meta, id) =>
    mediaManager.receiveTrackMeta(meta, id)
  )

  leaveAction.onMessage((_, id) =>
    pipe(
      peerManager.get(id),
      Effect.andThen(exists =>
        exists
          ? exists.scope.exit('PeerLeftRoom', {
              message: 'peer left room',
              peer: exists.handle,
              peerId: exists.peerId
            })
          : Effect.void
      ),
      Effect.runSync
    )
  )

  onPeer(
    flow(
      Effect.fnUntraced(function* (peer: PeerHandle, id: string) {
        const created = yield* makeRoomPeer({
          peerId: id,
          handle: peer,
          handshakeTimeoutMs,
          onHandshakeError
        })
        if (!created) {
          return
        }
        peerMap[id] = peer

        peer.setHandlers({
          stream: stream => mediaManager.receiveRemoteStream(id, stream),
          track: (track, stream) =>
            mediaManager.receiveRemoteTrack(id, track, stream),
          signal: sdp => {
            if (!activePeerMap[id]) {
              return
            }

            void signalAction.send(sdp as unknown as DataPayload, id)
          }
        })
      }, Effect.provideContext(RoomPeerContext)),
      Effect.runSync
    )
  )

  if (isBrowser) {
    unregisterBeforeUnloadCleanup = registerBeforeUnloadCleanup(() =>
      leave().catch(noOp)
    )
  }

  return {
    makeAction: actionManager.makeAction,

    leave,

    ping: async id => {
      if (!activePeerMap[id]) {
        throw mkErr(`no active peer with id ${id}`)
      }

      const start = Date.now()

      await new Promise<void>((resolve, reject) => {
        const queue = (pendingPongs[id] ??= [])

        const clearFromQueue = (): void => {
          const currentQueue = pendingPongs[id]

          if (!currentQueue) {
            return
          }

          const i = currentQueue.indexOf(waiter)

          if (i > -1) {
            currentQueue.splice(i, 1)
          }

          if (!currentQueue.length) {
            delete pendingPongs[id]
          }
        }

        const waiter: PendingPongWaiter = {
          resolve: () => {
            clearFromQueue()
            resolve()
          },
          reject: reason => {
            clearFromQueue()
            reject(reason)
          }
        }

        queue.push(waiter)
        void pingAction
          .send('', id)
          .catch(err => waiter.reject(toError(err, 'peer disconnected')))
      })

      return Date.now() - start
    },

    isPassive: () => isPassive,

    getPeers: () =>
      fromEntries(
        entries(activePeerMap).map(([id, peer]) => [id, peer.connection])
      ) as Record<string, RTCPeerConnection>,

    addStream: (stream, options: AddMediaOptions = {}) =>
      mediaManager.addStream(stream, options, streamMetaAction.send),

    removeStream: (stream, options = {}) => {
      mediaManager.removeStream(stream, options.target)
    },

    addTrack: (track, stream, options: AddMediaOptions = {}) =>
      mediaManager.addTrack(track, stream, options, trackMetaAction.send),

    removeTrack: (track, options = {}) => {
      mediaManager.removeTrack(track, options.target)
    },

    replaceTrack: (oldTrack, newTrack, options: AddMediaOptions = {}) =>
      mediaManager.replaceTrack(
        oldTrack,
        newTrack,
        options,
        trackMetaAction.send
      ),

    get onPeerJoin() {
      return listeners.onPeerJoin
    },

    set onPeerJoin(handler) {
      listeners.onPeerJoin = handler

      if (handler) {
        keys(activePeerMap).forEach(peerId => handler(peerId))
      }
    },

    get onPeerLeave() {
      return listeners.onPeerLeave
    },

    set onPeerLeave(handler) {
      listeners.onPeerLeave = handler
    },

    get onPeerStream() {
      return mediaManager.onPeerStream
    },

    set onPeerStream(handler) {
      mediaManager.onPeerStream = handler
    },

    get onPeerTrack() {
      return mediaManager.onPeerTrack
    },

    set onPeerTrack(handler) {
      mediaManager.onPeerTrack = handler
    }
  } satisfies Room
})
export default flow(make, Effect.runSync) as (
  onPeer: (f: (peer: PeerHandle, id: string) => void) => void,
  onPeerLeave: (id: string) => void,
  onSelfLeave: () => void
) => Room
