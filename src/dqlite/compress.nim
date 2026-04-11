## BLOB compression helpers for dqlite.
##
## Uses zippy (pure Nim, no C dependency) for compressing large text/JSON
## blobs before storing in SQLite BLOB columns.
##
## Import separately: `import dqlite/compress`
##
## Two usage modes:
## - Explicit: `compressBlob` / `decompressBlob` for direct control
## - Auto: `compressIfLarger` / `decompressAuto` with magic byte prefix
##   for transparent handling of mixed compressed/uncompressed data

import zippy
export ZippyError

const
  MagicUncompressed = '\x00'
  MagicCompressed = '\x01'

proc compressBlob*(data: string,
                   level: int = DefaultCompression): string {.raises: [ZippyError].} =
  ## Compress a string using deflate. Returns raw compressed bytes.
  zippy.compress(data, level, dfDeflate)

proc decompressBlob*(data: string): string {.raises: [ZippyError].} =
  ## Decompress deflate-compressed data.
  zippy.uncompress(data, dfDeflate)

proc compressIfLarger*(data: string, threshold: int = 1024,
                       level: int = DefaultCompression): string {.raises: [].} =
  ## Compress only if data exceeds threshold bytes.
  ## Prepends a 1-byte magic flag: 0x00 = uncompressed, 0x01 = compressed.
  ## Use `decompressAuto` to decompress.
  if data.len <= threshold:
    return MagicUncompressed & data
  try:
    let compressed = zippy.compress(data, level, dfDeflate)
    if compressed.len < data.len:
      return MagicCompressed & compressed
    else:
      return MagicUncompressed & data
  except ZippyError:
    return MagicUncompressed & data

proc decompressAuto*(data: string): string {.raises: [ZippyError].} =
  ## Decompress based on magic byte prefix.
  ## 0x01 = decompress (raises ZippyError on corrupt data).
  ## 0x00 = strip prefix and return as-is.
  ## No prefix = legacy data, return as-is.
  if data.len == 0:
    return ""
  if data[0] == MagicCompressed:
    return zippy.uncompress(data[1 .. ^1], dfDeflate)
  elif data[0] == MagicUncompressed:
    return data[1 .. ^1]
  else:
    # Legacy data without magic prefix — return as-is
    return data
