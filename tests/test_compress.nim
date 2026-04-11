import std/[unittest, strutils]
import dqlite/compress

suite "blob compression":
  test "Given JSON string When compressBlob Then smaller":
    let data = """{"assignments":{"node1":"true","node2":"false"}""" & ",".repeat(500)
    let compressed = compressBlob(data)
    check compressed.len < data.len

  test "Given compressed data When decompressBlob Then original restored":
    let original = "Hello, this is a test string for compression!"
    let compressed = compressBlob(original)
    let restored = decompressBlob(compressed)
    check restored == original

  test "Given empty string When compressBlob Then handles gracefully":
    let compressed = compressBlob("")
    let restored = decompressBlob(compressed)
    check restored == ""

  test "Given small data When compressIfLarger Then not compressed":
    let data = "short"
    let result = compressIfLarger(data, threshold = 1024)
    check result[0] == '\x00'  # MagicUncompressed
    check result[1 .. ^1] == data

  test "Given large data When compressIfLarger Then compressed with magic flag":
    let data = "x".repeat(2000)
    let result = compressIfLarger(data, threshold = 1024)
    check result[0] == '\x01'  # MagicCompressed
    check result.len < data.len

  test "Given compressed-flagged data When decompressAuto Then decompressed":
    let original = "y".repeat(2000)
    let flagged = compressIfLarger(original, threshold = 1024)
    let restored = decompressAuto(flagged)
    check restored == original

  test "Given uncompressed-flagged data When decompressAuto Then returned as-is":
    let original = "short"
    let flagged = compressIfLarger(original, threshold = 1024)
    let restored = decompressAuto(flagged)
    check restored == original

  test "Given empty string When decompressAuto Then returns empty":
    check decompressAuto("") == ""

  test "Given legacy data without magic prefix When decompressAuto Then returned as-is":
    let legacy = "some old uncompressed text"
    check decompressAuto(legacy) == legacy

  test "Given corrupt compressed data When decompressAuto Then raises ZippyError":
    # Magic byte 0x01 (compressed) + garbage data
    let corrupt = "\x01\xFF\xFE\xFD\xFC\xFB"
    expect(ZippyError):
      discard decompressAuto(corrupt)

  test "Given large JSON When roundtrip Then data preserved":
    var json = """{"sessionId":"test","hash":"abc123","assignments":{"""
    for i in 0 ..< 100:
      if i > 0: json.add(",")
      json.add("\"node" & $i & "\":\"true\"")
    json.add("}}")

    let compressed = compressIfLarger(json, threshold = 512)
    check compressed[0] == '\x01'
    let restored = decompressAuto(compressed)
    check restored == json
