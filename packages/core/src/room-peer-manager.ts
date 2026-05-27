import {Context, Effect, pipe, Stream, Ref, PubSub} from 'effect'

import {RoomPeerEvent, RoomPeerState} from './room-peer'

const make = Effect.fn(function* () {
  const peerRepository = new Map<string, Ref.Ref<RoomPeerState>>()
  const eventHub = yield* PubSub.sliding<RoomPeerEvent>({
    capacity: 16
  })
  yield* Effect.addFinalizer(() =>
    pipe(
      Effect.all([
        Effect.sync(() => peerRepository.clear()),
        PubSub.shutdown(eventHub)
      ])
    )
  )
  return {
    add: Effect.fnUntraced(function* (
      peerId: string,
      peerStateRef: Ref.Ref<RoomPeerState>
    ) {
      const incomingState = yield* Ref.get(peerStateRef)
      const existingRef = peerRepository.get(peerId)
      if (existingRef) {
        if (existingRef === peerStateRef) {
          return false // unchanged
        }

        const existing = yield* Ref.get(existingRef)
        yield* existing.scope.exit('PeerReplaced', {
          peerId,
          current: existing.handle,
          next: incomingState.handle,
          message: 'peer replaced'
        })
      }
      peerRepository.set(peerId, peerStateRef)
      yield* incomingState.scope.addFinalizerExit(() =>
        Effect.sync(() => {
          peerRepository.delete(peerId)
        })
      )
      return true
    }),
    getRef: (peerId: string) => {
      const existing = peerRepository.get(peerId)
      return existing ?? null
    },
    get: (peerId: string) =>
      pipe(peerRepository.get(peerId), ref =>
        Effect.suspend(() => (ref ? Ref.get(ref) : Effect.succeed(null)))
      ),
    getAllRefs: () => peerRepository.values(),
    getAll: (options?: {activeOnly?: boolean | undefined}) =>
      Effect.gen(function* () {
        const peers = yield* Effect.all(
          [...peerRepository.values()].map(Ref.get)
        )
        return options?.activeOnly
          ? peers.filter(RoomPeerState.$is('Active'))
          : peers
      }),
    events: {
      publish: (event: RoomPeerEvent) => PubSub.publish(eventHub, event),
      stream: Stream.fromPubSub(eventHub)
    }
  }
})
export type RoomPeerManagerT = Effect.Success<ReturnType<typeof make>>
export class RoomPeerManager extends Context.Service<
  RoomPeerManager,
  RoomPeerManagerT
>()('RoomPeerManager') {
  static make = () => Effect.map(make(), this.of)
}
