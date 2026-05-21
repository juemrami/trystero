import {alloc, candidateType, resetTimer, toError} from './utils'
import type {BaseRoomConfig, PeerHandle, PeerHandlers, Signal} from './types'
import {
  Context,
  Effect,
  Exit,
  flow,
  Match,
  Option,
  pipe,
  Result,
  Scope,
  Stream
} from 'effect'
import * as RtcPeerConnection from './webrtc/RtcPeerConnection'

const iceTimeout = 15_000
const disconnectedCloseDelayMs = 5_000
const offerType = 'offer'
const answerType = 'answer'
const outOfRangePattern = /out of range/i

type SdpDescription = {
  type: RTCSdpType
  sdp: string
}

const rewriteMdnsCandidatesToLoopback = (sdp: string): string =>
  sdp.replace(/ (\S+\.local) (\d+) typ host/g, ' 127.0.0.1 $2 typ host')

export default (
  initiator: boolean,
  {
    trickleIce,
    rtcConfig,
    rtcPolyfill,
    turnConfig,
    _test_only_mdnsHostFallbackToLoopback
  }: BaseRoomConfig
): PeerHandle => {
  rtcConfig = {
    ...rtcConfig,
    iceServers: defaultIceServers.concat(turnConfig ?? [])
  }
  const scope = Scope.makeUnsafe()
  const handleScope = Context.make(Scope.Scope, scope)
  const pc = pipe(
    rtcPolyfill
      ? RtcPeerConnection.makePolyFill(rtcPolyfill, rtcConfig)
      : RtcPeerConnection.makeGlobalThis(rtcConfig),
    Effect.provideContext(handleScope),
    Effect.runSync
  )
  /** wip refactor handle to raw PeerConnection */
  const _pc = Effect.runSync(pc.useUnsafe(Effect.succeed))

  const handlers: PeerHandlers = {}
  const pendingSignals: Signal[] = []
  const pendingData: ArrayBuffer[] = []
  const shouldTrickleIce = trickleIce !== false
  const pendingRemoteCandidates: RTCIceCandidateInit[] = []
  const pendingTracks: Array<{track: MediaStreamTrack; stream: MediaStream}> =
    []
  let makingOffer = false
  let isSettingRemoteAnswerPending = false
  let dataChannel: RTCDataChannel | null = null
  let disconnectedCloseTimer: number | null = null
  let didEmitClose = false

  const clearDisconnectedCloseTimer = (): null =>
    (disconnectedCloseTimer = resetTimer(disconnectedCloseTimer))

  const emitClose = (): void => {
    if (didEmitClose) {
      return
    }

    didEmitClose = true
    clearDisconnectedCloseTimer()
    handlers.close?.()
  }

  const emitSignal = (signal: Signal): void => {
    if (handlers.signal) {
      handlers.signal(signal)
    } else {
      pendingSignals.push(signal)
    }
  }

  const appendSignalHandler = (handler: (signal: Signal) => void): void => {
    const previousSignalHandler = handlers.signal

    handlers.signal = signal => {
      previousSignalHandler?.(signal)
      handler(signal)
    }

    if (pendingSignals.length > 0) {
      const queuedSignals = pendingSignals.splice(0)
      queuedSignals.forEach(signal => handlers.signal?.(signal))
    }
  }

  const normalizeSdp = (sdp: string): string =>
    _test_only_mdnsHostFallbackToLoopback
      ? rewriteMdnsCandidatesToLoopback(sdp)
      : sdp

  const normalizeCandidate = (
    candidate: RTCIceCandidateInit
  ): RTCIceCandidateInit => {
    if (
      !_test_only_mdnsHostFallbackToLoopback ||
      typeof candidate.candidate !== 'string'
    ) {
      return candidate
    }

    const normalizedCandidate = rewriteMdnsCandidatesToLoopback(
      candidate.candidate
    )

    return normalizedCandidate === candidate.candidate
      ? candidate
      : {...candidate, candidate: normalizedCandidate}
  }

  const localDescriptionSignal = (
    peerConnection: RTCPeerConnection
  ): SdpDescription => ({
    type: (peerConnection.localDescription?.type ?? offerType) as RTCSdpType,
    sdp: normalizeSdp(peerConnection.localDescription?.sdp ?? '')
  })

  const canApplyRemoteCandidate = function (
    remoteDesc: RTCSessionDescription,
    candidate: RTCIceCandidateInit
  ) {
    if (remoteDesc === null) {
      return false
    }

    const numRemoteMediaSections = remoteDesc.sdp.match(/^m=/gm)?.length ?? 0

    if (
      typeof candidate.sdpMLineIndex === 'number' &&
      numRemoteMediaSections > 0 &&
      candidate.sdpMLineIndex >= numRemoteMediaSections
    ) {
      return false
    }

    const remoteUfrag = (() => {
      const match = remoteDesc.sdp.match(/a=ice-ufrag:([^\s]+)/)
      return match?.[1] ? match[1] : null
    })()

    if (
      remoteUfrag &&
      candidate.usernameFragment &&
      candidate.usernameFragment !== remoteUfrag
    ) {
      return false
    }

    return true
  }

  const addIceCandidate = Effect.fnUntraced(function* (
    candidate: RTCIceCandidateInit
  ) {
    return yield* pipe(
      Effect.result(pc.addIceCandidate(candidate)),
      Effect.andThen(
        Result.match({
          onSuccess: () => Effect.succeed(true),
          onFailure: error => {
            // https://developer.mozilla.org/en-US/docs/Web/API/RTCPeerConnection/addIceCandidate#exceptions
            // see 2nd scenario for `OperationError` DomException. candidate should get pended instead of erroring.
            if (
              error.cause instanceof Error &&
              outOfRangePattern.test(error.cause.message) &&
              typeof candidate.sdpMLineIndex === 'number'
            ) {
              return Effect.succeed(false)
            }
            return Effect.fail(error)
          }
        })
      )
    )
  })

  const flushPendingRemoteCandidates = Effect.gen(function* () {
    const remoteDesc = yield* pc.remoteDescription
    if (!remoteDesc || pendingRemoteCandidates.length === 0) {
      return
    }

    const queuedCandidates = pendingRemoteCandidates.splice(0)
    const stillPending: RTCIceCandidateInit[] = []

    for (const candidate of queuedCandidates) {
      if (!canApplyRemoteCandidate(remoteDesc, candidate)) {
        stillPending.push(candidate)
        continue
      }

      const didApply = yield* addIceCandidate(candidate)
      if (!didApply) {
        stillPending.push(candidate)
      }
    }

    if (stillPending.length > 0) {
      pendingRemoteCandidates.push(...stillPending)
    }
  })

  const addRemoteCandidate = Effect.fnUntraced(function* (
    candidate: RTCIceCandidateInit
  ) {
    const remoteDesc = yield* pc.remoteDescription
    if (remoteDesc && canApplyRemoteCandidate(remoteDesc, candidate)) {
      const didApply = yield* addIceCandidate(candidate)
      if (!didApply) {
        pendingRemoteCandidates.push(candidate)
      }
      return
    }
    pendingRemoteCandidates.push(candidate)
  })

  const setupDataChannel = (channel: RTCDataChannel): void => {
    channel.binaryType = 'arraybuffer'
    channel.bufferedAmountLowThreshold = 0xffff
    channel.onmessage = e => {
      const data = e.data as ArrayBuffer

      if (handlers.data) {
        handlers.data(data)
      } else {
        pendingData.push(data)
      }
    }
    channel.onopen = () => handlers.connect?.()
    channel.onclose = emitClose
    channel.onerror = ({error}) =>
      handlers.error?.(toError(error, 'data channel error'))
  }

  const emitLocalDescriptionSignal = (
    shouldTrickleIce: boolean
  ): Effect.Effect<SdpDescription> =>
    Effect.gen(function* () {
      const localDescSignal = pc.useUnsafe(
        flow(localDescriptionSignal, Effect.succeed)
      )
      const signal = yield* Effect.suspend(() =>
        shouldTrickleIce
          ? localDescSignal
          : RtcPeerConnection.waitForIceGathering(pc, iceTimeout).pipe(
              Effect.catchTag('TimeoutError', () => Effect.void),
              Effect.andThen(() => localDescSignal)
            )
      )
      emitSignal(signal)
      return signal
    })

  /** Initiates offer signal */
  const createOffer: (
    restartIce?: boolean
  ) => Effect.Effect<
    Result.Result<
      Signal,
      RtcPeerConnection.RtcPeerConnectionError | 'ConnectionClosed'
    >
  > = Effect.fnUntraced(function* (restartIce = false) {
    if ((yield* pc.connectionState) === 'closed') {
      return Result.fail('ConnectionClosed')
    }
    const offerResult = yield* Effect.result(
      Effect.gen(function* () {
        makingOffer = true
        yield* pc.useUnsafe(({signalingState, localDescription}) =>
          signalingState !== 'stable' &&
          signalingState !== 'closed' &&
          localDescription?.type === offerType
            ? pc.setLocalDescription({type: 'rollback'})
            : Effect.void
        )
        if (restartIce) {
          yield* pc.restartIce
          return yield* pipe(
            pc.createOffer({iceRestart: true}),
            Effect.andThen(pc.setLocalDescription)
          )
        }
        return yield* pc.setLocalDescription()
      })
    ).pipe(Effect.tap(() => Effect.sync(() => (makingOffer = false))))
    return yield* Result.match(offerResult, {
      onSuccess: () =>
        emitLocalDescriptionSignal(shouldTrickleIce).pipe(
          Effect.map(Result.succeed)
        ),
      onFailure: failure => {
        handlers.error?.(toError(failure.cause, 'failed to create local offer'))
        return Effect.succeed(Result.fail(failure))
      }
    })
  })

  const onNegotiationNeeded = createOffer(false)

  Effect.runSyncWith(handleScope)(
    Effect.gen(function* () {
      if (initiator) {
        dataChannel = yield* pc.createDataChannel('data')
        setupDataChannel(dataChannel)
      }
      yield* pc.useUnsafe(pc =>
        Effect.sync(() => {
          // note: due to task scheduling issues with Queue backed streams and the room handler callbacks,
          // we need to ensure early track/datachannel event callbacks are ran in the same event loop as this peer handle's constructor
          if (!initiator) {
            pc.addEventListener('datachannel', ({channel}) => {
              dataChannel = channel
              setupDataChannel(channel)
            })
          }
          pc.addEventListener('track', e => {
            const stream = e.streams[0]
            if (stream) {
              if (!handlers.track && !handlers.stream) {
                pendingTracks.push({track: e.track, stream})
                return
              }

              handlers.track?.(e.track, stream)
              handlers.stream?.(stream)
            }
          })
        })
      )
      yield* Effect.forkScoped(
        pipe(
          RtcPeerConnection.makeStreamFromEventListeners(pc, [
            'negotiationneeded',
            'icecandidate',
            'connectionstatechange',
            'removestream'
          ]),
          Stream.mapEffect(event =>
            Match.value(event).pipe(
              Match.tagsExhaustive({
                // _pc.onnegotiationneeded = async () => Effect.runPromise(createOffer(false))
                negotiationneeded: () => onNegotiationNeeded,
                // _pc.onicecandidate = ({candidate}) => {
                //   if (!shouldTrickleIce || !candidate) {
                //     return
                //   }

                //   const candidatePayload = normalizeCandidate(
                //     typeof candidate.toJSON === 'function'
                //       ? candidate.toJSON()
                //       : {
                //           candidate: candidate.candidate,
                //           sdpMid: candidate.sdpMid,
                //           sdpMLineIndex: candidate.sdpMLineIndex,
                //           usernameFragment: candidate.usernameFragment
                //         }
                //   )

                //   emitSignal({
                //     type: candidateType,
                //     sdp: JSON.stringify(candidatePayload)
                //   })
                // }
                icecandidate: ({candidate}) =>
                  Effect.gen(function* () {
                    yield* Effect.void
                    if (!shouldTrickleIce || !candidate) {
                      return
                    }
                    const candidatePayload = normalizeCandidate(
                      typeof candidate.toJSON === 'function' // for polyfills??
                        ? candidate.toJSON()
                        : {
                            candidate: candidate.candidate,
                            sdpMid: candidate.sdpMid,
                            sdpMLineIndex: candidate.sdpMLineIndex,
                            usernameFragment: candidate.usernameFragment
                          }
                    )
                    emitSignal({
                      type: candidateType,
                      sdp: JSON.stringify(candidatePayload)
                    })
                  }),
                // _pc.onconnectionstatechange = () => {
                //   if (
                //     _pc.connectionState === 'connected' ||
                //     _pc.connectionState === 'connecting'
                //   ) {
                //     clearDisconnectedCloseTimer()
                //     return
                //   }

                //   if (_pc.connectionState === 'disconnected') {
                //     if (!disconnectedCloseTimer) {
                //       disconnectedCloseTimer = setTimeout(() => {
                //         disconnectedCloseTimer = null

                //         if (_pc.connectionState === 'disconnected') {
                //           emitClose()
                //         }
                //       }, disconnectedCloseDelayMs)
                //     }

                //     return
                //   }

                //   if (_pc.connectionState === 'failed' || _pc.connectionState === 'closed') {
                //     emitClose()
                //   }
                // }
                connectionstatechange: () =>
                  Effect.map(pc.connectionState, state =>
                    pipe(
                      Match.value(state),
                      Match.when('new', () => {}),
                      Match.whenOr(
                        'connected',
                        'connecting',
                        clearDisconnectedCloseTimer
                      ),
                      Match.whenOr('failed', 'closed', emitClose),
                      Match.when('disconnected', () => {
                        if (!disconnectedCloseTimer) {
                          disconnectedCloseTimer = setTimeout(() => {
                            disconnectedCloseTimer = null
                            const state = pc.connectionState.pipe(
                              Effect.runSync
                            )
                            if (state === 'disconnected') {
                              emitClose()
                            }
                          }, disconnectedCloseDelayMs)
                        }
                      }),
                      Match.exhaustive
                    )
                  ),
                // ;(
                //   _pc as RTCPeerConnection & {
                //     onremovestream: ((e: {stream: MediaStream}) => void) | null
                //   }
                // ).onremovestream = e => handlers.stream?.(e.stream)
                /** todo: remove
                 * wiki says "Instead of listening for this obsolete event, you should listen for removetrack events on each stream."
                 */
                removestream: event =>
                  Effect.sync(() =>
                    event.stream ? handlers.stream?.(event.stream) : undefined
                  )
              })
            )
          ),
          Stream.runDrain
        ).pipe(),
        {startImmediately: true}
      )
    })
  )
  /** @deprecated */
  const _offerPromise = initiator
    ? new Promise<Signal | void>(res =>
        appendSignalHandler(signal => {
          if (signal.type === offerType) {
            res(signal)
          }
        })
      )
    : Promise.resolve()

  const offerPromise = Effect.callback<Signal | void>(res =>
    initiator
      ? appendSignalHandler(signal => {
          if (signal.type === offerType) {
            res(Effect.succeed(signal))
          }
        })
      : res(Effect.void)
  )

  if (initiator) {
    queueMicrotask(() => {
      Effect.runPromise(
        pc.useUnsafe(pc => {
          if (
            !makingOffer &&
            pc.signalingState === 'stable' &&
            !pc.localDescription &&
            pc.connectionState !== 'closed'
          ) {
            return onNegotiationNeeded
          }
          return Effect.void
        })
      )
    })
  }

  return {
    created: Date.now(),

    connection: pc.useUnsafe(Effect.succeed).pipe(Effect.runSync),

    get channel(): RTCDataChannel | null {
      return dataChannel
    },

    get isDead(): boolean {
      return pc.connectionState.pipe(
        Effect.map(state => state === 'closed'),
        Effect.runSync
      )
    },

    getOffer: async (restartIce = false): Promise<Signal | void> =>
      Effect.gen(function* () {
        if (!initiator) {
          return
        }
        if (restartIce) {
          yield* createOffer(true)
        }
        if ((yield* pc.localDescription)?.type === offerType) {
          const signalFromLocalDesc = pc.useUnsafe(
            flow(localDescriptionSignal, Effect.succeed)
          )
          const offeredSignal: Signal = yield* Effect.suspend(() =>
            shouldTrickleIce
              ? signalFromLocalDesc
              : RtcPeerConnection.waitForIceGathering(pc, iceTimeout).pipe(
                  Effect.catchTag('TimeoutError', () => Effect.void),
                  Effect.andThen(() => signalFromLocalDesc)
                )
          )
          return offeredSignal
        }
        return yield* offerPromise
      }).pipe(Effect.runPromise),

    async signal(sdp: Signal): Promise<Signal | void> {
      return Effect.gen(function* () {
        if (sdp.type === candidateType) {
          const candidate = yield* Effect.try({
            try: () => JSON.parse(sdp.sdp) as RTCIceCandidateInit | null,
            catch: err => {
              handlers.error?.(toError(err, 'failed to parse remote candidate'))
            }
          })
          if (candidate && typeof candidate === 'object') {
            yield* addRemoteCandidate(normalizeCandidate(candidate))
          }
          return
        }

        if (
          dataChannel?.readyState === 'open' &&
          !sdp.sdp?.includes('a=rtpmap')
        ) {
          return
        }

        const rtcSdp: RTCSessionDescriptionInit = {
          ...sdp,
          sdp: normalizeSdp(sdp.sdp)
        }

        if (sdp.type === offerType) {
          if (
            makingOffer ||
            ((yield* pc.signalingState) !== 'stable' &&
              !isSettingRemoteAnswerPending)
          ) {
            if (initiator) {
              return
            }

            yield* Effect.all([
              pc.setLocalDescription({type: 'rollback'}),
              pc.setRemoteDescription(rtcSdp)
            ])
          } else {
            yield* pc.setRemoteDescription(rtcSdp)
          }

          yield* flushPendingRemoteCandidates
          yield* pc.setLocalDescription()
          const answer = yield* emitLocalDescriptionSignal(shouldTrickleIce)
          return answer
        }

        if (sdp.type === answerType) {
          isSettingRemoteAnswerPending = true
          yield* Effect.all([
            pc.setRemoteDescription(rtcSdp),
            flushPendingRemoteCandidates
          ]).pipe(
            Effect.onExit(() =>
              Effect.sync(() => (isSettingRemoteAnswerPending = false))
            )
          )
        }
        return yield* Effect.void
      }).pipe(
        Effect.tapError(err =>
          Effect.sync(() =>
            handlers.error?.(toError(err, 'failed to apply remote signal'))
          )
        ),
        Effect.runPromise
      )
    },

    sendData: data => dataChannel?.send(data as unknown as never),

    destroy: () => {
      clearDisconnectedCloseTimer()
      dataChannel?.close()
      makingOffer = false
      isSettingRemoteAnswerPending = false
      emitClose()
      Effect.runSync(Scope.close(scope, Exit.succeed(undefined)))
    },

    setHandlers: newHandlers => {
      const {signal, ...restHandlers} = newHandlers
      Object.assign(handlers, restHandlers)

      if (handlers.data && pendingData.length > 0) {
        const queued = pendingData.splice(0)
        queued.forEach(data => handlers.data?.(data))
      }

      if (signal) {
        appendSignalHandler(signal)
      }

      if ((handlers.track || handlers.stream) && pendingTracks.length > 0) {
        const queued = pendingTracks.splice(0)
        queued.forEach(({track, stream}) => {
          handlers.track?.(track, stream)
          handlers.stream?.(stream)
        })
      }
    },

    offerPromise: offerPromise.pipe(Effect.runPromise),

    addStream: stream => pc.addStream(stream).pipe(Effect.runSync),

    removeStream: stream => pc.removeStream(stream).pipe(Effect.runSync),

    addTrack: (track, stream) =>
      pc.addTrack(track, stream).pipe(Effect.runSync),

    removeTrack: (track: MediaStreamTrack) =>
      pipe(
        pc.getSenders,
        Effect.map(senders => senders.find(s => s.track === track)),
        Effect.map(Option.fromUndefinedOr),
        Effect.map(Option.map(sender => pc.removeTrack(sender))),
        Effect.runSync
      ),

    replaceTrack: (oldTrack, newTrack) =>
      pipe(
        pc.replaceTrack(oldTrack, newTrack),
        Effect.asVoid,
        Effect.runPromise
      )
  }
}

export const defaultIceServers: RTCIceServer[] = [
  ...alloc(3, (_, i) => `stun:stun${i || ''}.l.google.com:19302`),
  'stun:stun.cloudflare.com:3478'
].map(url => ({urls: url}))
