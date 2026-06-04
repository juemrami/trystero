import {
  entries,
  fromEntries,
  isBrowser,
  keys,
  libName,
  mkErr,
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
  Signal,
  TargetPeers
} from './types'
import {
  Array,
  Context,
  Effect,
  Exit,
  Fiber,
  flow,
  Function,
  identity,
  pipe,
  Scope,
  Stream
} from 'effect'
import {
  ActionWireEvent,
  ActionWireManager,
  type ActionWire
} from './action-wire'
import {RoomPeerManager} from './room-peer-manager'
import {makeRoomPeer, RoomPeerExitValue, RoomPeerEvent} from './room-peer'

const unloadEvent = 'beforeunload'
const defaultHandshakeTimeoutMs = 10_000
const internalNs = (ns: string): string => '@_' + ns

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

  const makeActionInternal = flow(
    wireManager.makeActionWire,
    Scope.provide(roomScope)
  )

  const normalizeTargetPeers = Effect.fn(function* (
    target: TargetPeers,
    includePending?: boolean
  ) {
    return yield* target
      ? Effect.succeed(Array.isArray(target) ? target : [target])
      : Effect.map(
          peerManager.getAll({
            activeOnly: !includePending
          }),
          found => found.map(p => p.peerId)
        )
  })

  /** awaits action's ReceiveComplete event and runs the given callback */
  const onWireReceivedListener = <A, E, T extends DataPayload>(args: {
    wire: ActionWire<T>
    listener: (payload: T, peerId: string) => Effect.Effect<A, E>
    take?: number /** ends stream listener once N events have been emitted */
  }) =>
    Stream.runDrain(
      args.wire.events.pipe(
        Stream.filter(ActionWireEvent.$is('ReceiveComplete')),
        Stream.tap(({payload, peerId}) => args.listener(payload as T, peerId)),
        args.take ? Stream.take(args.take) : identity
      )
    )

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

  const pingAction = yield* makeActionInternal<string>(internalNs('ping'))
  const pongAction = yield* makeActionInternal<string>(internalNs('pong'))
  const signalAction = yield* makeActionInternal<Signal>(internalNs('signal'))
  const streamMetaAction = yield* makeActionInternal<InternalMediaMeta>(
    internalNs('stream')
  )
  const trackMetaAction = yield* makeActionInternal<InternalMediaMeta>(
    internalNs('track')
  )
  const leaveAction = yield* makeActionInternal<string>(internalNs('leave'), {
    sendToPending: true,
    receiveWhilePending: true
  })

  const replyWithPongFiber = yield* onWireReceivedListener({
    wire: pingAction,
    listener: (_, id) => pongAction.send('', [id])
  }).pipe(Effect.forkIn(roomScope))

  yield* onWireReceivedListener({
    wire: pongAction,
    listener: (_, id) =>
      Effect.sync(() => {
        const queue = pendingPongs[id]
        const waiter = queue?.shift()

        waiter?.resolve()

        if (queue && !queue.length) {
          delete pendingPongs[id]
        }
      })
  }).pipe(Effect.forkIn(roomScope))

  yield* onWireReceivedListener({
    wire: signalAction,
    listener: (sdp, id) =>
      Effect.sync(() => {
        if (!activePeerMap[id]) {
          return
        }

        void peerMap[id]?.signal(sdp as never)
      })
  }).pipe(Effect.forkIn(roomScope))

  yield* onWireReceivedListener({
    wire: streamMetaAction,
    listener: (meta, id) =>
      Effect.sync(() => mediaManager.receiveStreamMeta(meta, id))
  }).pipe(Effect.forkIn(roomScope))

  yield* onWireReceivedListener({
    wire: trackMetaAction,
    listener: (meta, id) =>
      Effect.sync(() => mediaManager.receiveTrackMeta(meta, id))
  }).pipe(Effect.forkIn(roomScope))

  yield* onWireReceivedListener({
    wire: leaveAction,
    listener: (_, id) =>
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
        )
      )
  }).pipe(Effect.forkIn(roomScope))

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

            signalAction.send(sdp, [id]).pipe(Effect.runPromise)
          }
        })
      }, Effect.provideContext(RoomPeerContext)),
      Effect.runSync
    )
  )

  const mediaManagerActionAdapter =
    <T extends DataPayload>(action: ActionWire<T>) =>
    (data: T, target: TargetPeers) =>
      Effect.gen(function* () {
        const targets = yield* normalizeTargetPeers(target)
        const fibers = yield* action.send(data, targets)
        return fibers.map(Function.constVoid)
      }).pipe(Effect.runPromise)

  const sendTrackMeta = mediaManagerActionAdapter(trackMetaAction)
  const sendStreamMeta = mediaManagerActionAdapter(streamMetaAction)

  const leaveRoom = Effect.gen(function* () {
    const peerIds = (yield* peerManager.getAll()).map(p => p.peerId)
    const sendLeaveResult = yield* leaveAction
      .send('', peerIds)
      .pipe(Effect.result)
    if (sendLeaveResult._tag === 'Success') {
      yield* Effect.sleep(99) // todo: determine if necessary
    }
    yield* Scope.close(
      roomScope,
      Exit.succeed(RoomPeerExitValue.RoomLeft({message: 'room left'}))
    )
    onSelfLeave()
  })

  if (isBrowser) {
    yield* Effect.forkIn(roomScope)(
      Stream.runDrain(
        Stream.fromEventListener(globalThis.window, unloadEvent).pipe(
          Stream.tap(() => leaveRoom),
          Stream.take(1) // cleanup stream listener after first event
        )
      )
    )
  }

  return {
    //@ts-ignore
    makeAction: (...args: Parameters<Room['makeAction']>) => {
      // temporary hack for `peer-lifecycle` disable auto-pong requirement.
      // remove once actionManager has been updated.
      if (args[0] === '@_ping') {
        Fiber.interrupt(replyWithPongFiber).pipe(Effect.runSync)
      }
      return actionManager.makeAction(...args)
    },

    leave: () => leaveRoom.pipe(Effect.runPromise),

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
          .send('', [id])
          .pipe(Effect.runPromise)
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
      mediaManager.addStream(stream, options, sendStreamMeta),

    removeStream: (stream, options = {}) => {
      mediaManager.removeStream(stream, options.target)
    },

    addTrack: (track, stream, options: AddMediaOptions = {}) =>
      mediaManager.addTrack(track, stream, options, sendTrackMeta),

    removeTrack: (track, options = {}) => {
      mediaManager.removeTrack(track, options.target)
    },

    replaceTrack: (oldTrack, newTrack, options: AddMediaOptions = {}) =>
      mediaManager.replaceTrack(oldTrack, newTrack, options, sendTrackMeta),

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
