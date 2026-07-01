import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';

const kOrderedTailHoldBytes = 1024;
const kOrderedTailWaitTimeout = Duration(seconds: 2);
const kOrderedTailReleaseDelay = Duration(milliseconds: 50);

/// Buffers the last [holdBytes] of [source], then waits for the tail gate before emitting it.
///
/// When [enabled] is false or [tailGateSendPort] is null, [source] is forwarded unchanged.
Stream<List<int>> wrapUploadStreamWithOrderedTail({
  required Stream<List<int>> source,
  required bool enabled,
  required int finishOrderIndex,
  required SendPort? tailGateSendPort,
  int holdBytes = kOrderedTailHoldBytes,
}) async* {
  if (!enabled || tailGateSendPort == null) {
    yield* source;
    return;
  }

  Future<void> acquireTailSlot() async {
    final response = ReceivePort();
    tailGateSendPort.send(<String, Object?>{
      't': 'tail',
      'i': finishOrderIndex,
      'r': response.sendPort,
    });
    try {
      await response.first.timeout(kOrderedTailWaitTimeout);
    } on TimeoutException {
      tailGateSendPort.send(<String, Object?>{
        't': 'force',
        'i': finishOrderIndex,
      });
    } finally {
      response.close();
    }
    await Future<void>.delayed(kOrderedTailReleaseDelay);
  }

  final buffer = <int>[];

  await for (final chunk in source) {
    if (chunk.isEmpty) {
      continue;
    }
    buffer.addAll(chunk);
    while (buffer.length > holdBytes) {
      final take = buffer.length - holdBytes;
      yield Uint8List.fromList(buffer.sublist(0, take));
      buffer.removeRange(0, take);
    }
  }

  if (buffer.isEmpty) {
    return;
  }

  await acquireTailSlot();
  yield Uint8List.fromList(buffer);
}
