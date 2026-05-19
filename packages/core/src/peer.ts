import {all, alloc, candidateType, resetTimer, toError} from './utils'
import type {BaseRoomConfig, PeerHandle, PeerHandlers, Signal} from './types'
import {Effect, Exit, flow, pipe, Result, Scope} from 'effect'
import * as RtcPeerConnection from './webrtc/RtcPeerConnection'

const iceTimeout = 15_000
const disconnectedCloseDelayMs = 5_000
const iceStateEvent = 'icegatheringstatechange'
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
  const pc = pipe(
    rtcPolyfill
      ? RtcPeerConnection.makePolyFill(rtcPolyfill, rtcConfig)
      : RtcPeerConnection.makeGlobalThis(rtcConfig),
    Effect.provideService(Scope.Scope, scope),
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
  /** @deprecated */
  const _getRemoteUfrag = (): string | null => {
    const sdp = _pc.remoteDescription?.sdp

    if (!sdp) {
      return null
    }

    const match = sdp.match(/a=ice-ufrag:([^\s]+)/)
    return match?.[1] ?? null
  }
  /** @deprecated */
  const _getRemoteMediaSectionCount = (): number =>
    (_pc.remoteDescription?.sdp?.match(/^m=/gm) ?? []).length

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
  /** @deprecated */
  const addIceCandidateSafe = async (
    candidate: RTCIceCandidateInit
  ): Promise<boolean> => {
    try {
      await _pc.addIceCandidate(candidate)
      return true
    } catch (err) {
      if (
        err instanceof Error &&
        outOfRangePattern.test(err.message) &&
        typeof candidate.sdpMLineIndex === 'number'
      ) {
        return false
      }

      throw err
    }
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
  /** @deprecated */
  const _flushPendingRemoteCandidates = async (): Promise<void> => {
    if (!_pc.remoteDescription || pendingRemoteCandidates.length === 0) {
      return
    }

    const queuedCandidates = pendingRemoteCandidates.splice(0)
    const stillPending: RTCIceCandidateInit[] = []

    for (const candidate of queuedCandidates) {
      if (!canApplyRemoteCandidate(_pc.remoteDescription!, candidate)) {
        stillPending.push(candidate)
        continue
      }

      const didApply = await addIceCandidateSafe(candidate)

      if (!didApply) {
        stillPending.push(candidate)
      }
    }

    if (stillPending.length > 0) {
      pendingRemoteCandidates.push(...stillPending)
    }
  }
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

  /** @deprecated */
  const _addRemoteCandidate = async (
    candidate: RTCIceCandidateInit
  ): Promise<void> => {
    if (canApplyRemoteCandidate(_pc.remoteDescription!, candidate)) {
      const didApply = await addIceCandidateSafe(candidate)

      if (!didApply) {
        pendingRemoteCandidates.push(candidate)
      }
      return
    }

    pendingRemoteCandidates.push(candidate)
  }
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
  /** @deprecated */
  const _waitForIceGathering = async (
    peerConnection: RTCPeerConnection
  ): Promise<SdpDescription> => {
    let timeout: ReturnType<typeof setTimeout> | null = null

    try {
      await Promise.race([
        new Promise<void>(res => {
          const checkState = (): void => {
            if (peerConnection.iceGatheringState === 'complete') {
              peerConnection.removeEventListener(iceStateEvent, checkState)
              res()
            }
          }

          peerConnection.addEventListener(iceStateEvent, checkState)
          checkState()
        }),
        new Promise<void>(res => {
          timeout = setTimeout(res, iceTimeout)
        })
      ])
    } finally {
      resetTimer(timeout)
    }

    return localDescriptionSignal(peerConnection)
  }
  /** @deprecated */
  const _emitLocalDescriptionSignal = async (): Promise<SdpDescription> => {
    const signal = shouldTrickleIce
      ? localDescriptionSignal(_pc)
      : await _waitForIceGathering(_pc)

    emitSignal(signal)
    return signal
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

  if (initiator) {
    dataChannel = _pc.createDataChannel('data')
    setupDataChannel(dataChannel)
  } else {
    _pc.ondatachannel = ({channel}) => {
      dataChannel = channel
      setupDataChannel(channel)
    }
  }
  /** @deprecated */
  const _createOffer = async (restartIce = false): Promise<Signal | void> => {
    if (_pc.connectionState === 'closed') {
      return
    }

    try {
      makingOffer = true

      if (restartIce) {
        if (
          _pc.signalingState !== 'stable' &&
          _pc.signalingState !== 'closed' &&
          _pc.localDescription?.type === offerType
        ) {
          await _pc.setLocalDescription({type: 'rollback'})
        }

        if (typeof _pc.restartIce === 'function') {
          _pc.restartIce()
        }
      }

      await _pc.setLocalDescription(
        restartIce ? await _pc.createOffer({iceRestart: true}) : undefined
      )
      const offer = await _emitLocalDescriptionSignal()
      return offer
    } catch (err) {
      handlers.error?.(toError(err, 'failed to create local offer'))
    } finally {
      makingOffer = false
    }
  }
  /** Initiates offer signal */
  const createOffer: (
    restartIce?: boolean
  ) => Effect.Effect<
    Result.Result<
      Signal,
      RtcPeerConnection.RtcPeerConnectionError | 'ConnectionClosed'
    >
  > = Effect.fnUntraced(function* (restartIce = false) {
    if (_pc.connectionState === 'closed') {
      return Result.fail('ConnectionClosed')
    }
    const offerResult = yield* Effect.result(
      Effect.gen(function* () {
        makingOffer = true
        if (
          _pc.signalingState !== 'stable' &&
          _pc.signalingState !== 'closed' &&
          _pc.localDescription?.type === offerType
        ) {
          yield* pc.setLocalDescription({type: 'rollback'})
        }
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

  _pc.onnegotiationneeded = async () => Effect.runPromise(createOffer(false))

  _pc.onicecandidate = ({candidate}) => {
    if (!shouldTrickleIce || !candidate) {
      return
    }

    const candidatePayload = normalizeCandidate(
      typeof candidate.toJSON === 'function'
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
  }
  _pc.onconnectionstatechange = () => {
    if (
      _pc.connectionState === 'connected' ||
      _pc.connectionState === 'connecting'
    ) {
      clearDisconnectedCloseTimer()
      return
    }

    if (_pc.connectionState === 'disconnected') {
      if (!disconnectedCloseTimer) {
        disconnectedCloseTimer = setTimeout(() => {
          disconnectedCloseTimer = null

          if (_pc.connectionState === 'disconnected') {
            emitClose()
          }
        }, disconnectedCloseDelayMs)
      }

      return
    }

    if (_pc.connectionState === 'failed' || _pc.connectionState === 'closed') {
      emitClose()
    }
  }

  _pc.ontrack = e => {
    const stream = e.streams[0]

    if (stream) {
      if (!handlers.track && !handlers.stream) {
        pendingTracks.push({track: e.track, stream})
        return
      }

      handlers.track?.(e.track, stream)
      handlers.stream?.(stream)
    }
  }
  ;(
    _pc as RTCPeerConnection & {
      onremovestream: ((e: {stream: MediaStream}) => void) | null
    }
  ).onremovestream = e => handlers.stream?.(e.stream)

  const offerPromise = initiator
    ? new Promise<Signal | void>(res =>
        appendSignalHandler(signal => {
          if (signal.type === offerType) {
            res(signal)
          }
        })
      )
    : Promise.resolve()

  if (initiator) {
    queueMicrotask(() => {
      if (
        !makingOffer &&
        _pc.signalingState === 'stable' &&
        !_pc.localDescription &&
        _pc.connectionState !== 'closed'
      ) {
        void _pc.onnegotiationneeded?.(new Event('negotiationneeded'))
      }
    })
  }

  return {
    created: Date.now(),

    connection: _pc,

    get channel(): RTCDataChannel | null {
      return dataChannel
    },

    get isDead(): boolean {
      return _pc.connectionState === 'closed'
    },

    getOffer: async (restartIce = false): Promise<Signal | void> =>
      Effect.gen(function* () {
        if (!initiator) {
          return
        }
        if (restartIce) {
          yield* createOffer(true)
        }
        if (_pc.localDescription?.type === offerType) {
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
        return yield* Effect.promise(() => offerPromise)
      }).pipe(Effect.runPromise),

    async signal(sdp: Signal): Promise<Signal | void> {
      if (sdp.type === candidateType) {
        try {
          const candidate = JSON.parse(sdp.sdp) as RTCIceCandidateInit | null

          if (candidate && typeof candidate === 'object') {
            await Effect.runPromise(
              addRemoteCandidate(normalizeCandidate(candidate))
            )
          }
        } catch (err) {
          handlers.error?.(toError(err, 'failed to parse remote candidate'))
        }

        return
      }

      if (
        dataChannel?.readyState === 'open' &&
        !sdp.sdp?.includes('a=rtpmap')
      ) {
        return
      }

      try {
        const rtcSdp: RTCSessionDescriptionInit = {
          ...sdp,
          sdp: normalizeSdp(sdp.sdp)
        }

        if (sdp.type === offerType) {
          if (
            makingOffer ||
            (_pc.signalingState !== 'stable' && !isSettingRemoteAnswerPending)
          ) {
            if (initiator) {
              return
            }

            await all([
              _pc.setLocalDescription({type: 'rollback'}),
              _pc.setRemoteDescription(rtcSdp)
            ])
          } else {
            await _pc.setRemoteDescription(rtcSdp)
          }

          await Effect.runPromise(flushPendingRemoteCandidates)
          await _pc.setLocalDescription()
          const answer = await Effect.runPromise(
            emitLocalDescriptionSignal(shouldTrickleIce)
          )

          return answer
        }

        if (sdp.type === answerType) {
          isSettingRemoteAnswerPending = true

          try {
            await _pc.setRemoteDescription(rtcSdp)
            await Effect.runPromise(flushPendingRemoteCandidates)
          } finally {
            isSettingRemoteAnswerPending = false
          }
        }
      } catch (err) {
        handlers.error?.(toError(err, 'failed to apply remote signal'))
      }
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

    offerPromise,

    addStream: stream =>
      stream.getTracks().forEach(track => _pc.addTrack(track, stream)),

    removeStream: stream =>
      _pc
        .getSenders()
        .filter(
          sender => sender.track && stream.getTracks().includes(sender.track)
        )
        .forEach(sender => _pc.removeTrack(sender)),

    addTrack: (track, stream) => _pc.addTrack(track, stream),

    removeTrack: track => {
      const sender = _pc.getSenders().find(s => s.track === track)

      if (sender) {
        _pc.removeTrack(sender)
      }
    },

    replaceTrack: (oldTrack, newTrack) => {
      const sender = _pc.getSenders().find(s => s.track === oldTrack)

      if (sender) {
        return sender.replaceTrack(newTrack)
      }

      return undefined
    }
  }
}

export const defaultIceServers: RTCIceServer[] = [
  ...alloc(3, (_, i) => `stun:stun${i || ''}.l.google.com:19302`),
  'stun:stun.cloudflare.com:3478'
].map(url => ({urls: url}))
