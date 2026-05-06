import 'dart:async';
import 'dart:isolate';

/// Coordinates ordered release of the last chunk of each file upload across parallel isolates.
///
/// Upload isolates send a message `{t:'tail', i:fileIndex, r:SendPort}`; this coordinator replies on
/// [r] when all strictly preceding files have fully completed (see [onFileUploadFinished]).
/// A `{t:'force', i:fileIndex}` message marks predecessors as virtually complete (timeout recovery).
class SendTailCoordinator {
  SendTailCoordinator();

  final ReceivePort _receivePort = ReceivePort();
  late final SendPort requestSendPort = _receivePort.sendPort;

  final Set<int> _fullyDone = {};
  StreamSubscription? _subscription;

  void start() {
    _subscription = _receivePort.listen(_onMessage);
  }

  void dispose() {
    final sub = _subscription;
    if (sub != null) {
      unawaited(sub.cancel());
    }
    _receivePort.close();
    _fullyDone.clear();
  }

  /// Call when the HTTP upload for [fileIndex] has finished (success or failure).
  void onFileUploadFinished(int fileIndex) {
    _fullyDone.add(fileIndex);
    _releaseEligible();
  }

  void _onMessage(dynamic message) {
    if (message is! Map) {
      return;
    }
    final type = message['t'];
    if (type == 'tail') {
      final index = message['i'] as int?;
      final reply = message['r'] as SendPort?;
      if (index == null || reply == null) {
        return;
      }
      _onTailRequest(index, reply);
    } else if (type == 'force') {
      final index = message['i'] as int?;
      if (index == null) {
        return;
      }
      for (var i = 0; i < index; i++) {
        _fullyDone.add(i);
      }
      _releaseEligible();
    }
  }

  bool _predecessorsDone(int fileIndex) {
    for (var i = 0; i < fileIndex; i++) {
      if (!_fullyDone.contains(i)) {
        return false;
      }
    }
    return true;
  }

  final Map<int, List<SendPort>> _pending = {};

  void _onTailRequest(int fileIndex, SendPort reply) {
    if (_predecessorsDone(fileIndex)) {
      reply.send(true);
      return;
    }
    _pending.putIfAbsent(fileIndex, () => []).add(reply);
  }

  void _releaseEligible() {
    for (final index in _pending.keys.toList()) {
      if (_predecessorsDone(index)) {
        final waiters = _pending.remove(index);
        for (final reply in waiters ?? const <SendPort>[]) {
          reply.send(true);
        }
      }
    }
  }
}
