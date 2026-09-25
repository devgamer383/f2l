import os
import math
import asyncio
import aiofiles
from logging import getLogger
from hydrogram import raw

logger = getLogger('bot')

PART_SIZE = 512 * 1024  # 512 KB MTProto part size
BIG_FILE_THRESHOLD = 10 * 1024 * 1024  # 10 MB

async def fast_upload(client, file_path: str, progress=None, concurrency: int = 8):
    """
    High-Speed Parallel MTProto Chunk Uploader for Telegram.
    Uploads up to `concurrency` (default 8) 512KB parts simultaneously.
    Bypasses Pyrogram's sequential single-chunk upload bottleneck (3 MB/s)
    to achieve 20 - 40 MB/s upload speeds.
    """
    file_size = os.path.getsize(file_path)
    total_parts = math.ceil(file_size / PART_SIZE) if file_size > 0 else 1
    file_id = client.rnd_id()
    is_big = file_size > BIG_FILE_THRESHOLD
    file_name = os.path.basename(file_path)

    queue = asyncio.Queue(maxsize=concurrency * 2)
    uploaded_bytes = 0
    progress_lock = asyncio.Lock()

    async def worker():
        nonlocal uploaded_bytes
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            part_idx, chunk = item
            try:
                if is_big:
                    req = raw.functions.upload.SaveBigFilePart(
                        file_id=file_id,
                        file_part=part_idx,
                        file_total_parts=total_parts,
                        bytes=chunk
                    )
                else:
                    req = raw.functions.upload.SaveFilePart(
                        file_id=file_id,
                        file_part=part_idx,
                        bytes=chunk
                    )
                await client.invoke(req)
            except Exception as e:
                logger.error(f"Error uploading part {part_idx}: {e}")
                queue.task_done()
                raise e

            async with progress_lock:
                uploaded_bytes += len(chunk)
                if progress:
                    try:
                        await progress(uploaded_bytes, file_size)
                    except Exception:
                        pass

            queue.task_done()

    # Start parallel chunk workers
    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]

    try:
        async with aiofiles.open(file_path, "rb") as f:
            part_index = 0
            while True:
                chunk = await f.read(PART_SIZE)
                if not chunk:
                    break
                await queue.put((part_index, chunk))
                part_index += 1

        # Wait until all chunks in queue are uploaded
        await queue.join()
    finally:
        # Shutdown workers
        for _ in range(concurrency):
            await queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)

    if is_big:
        return raw.types.InputFileBig(
            id=file_id,
            parts=total_parts,
            name=file_name
        )
    else:
        return raw.types.InputFile(
            id=file_id,
            parts=total_parts,
            name=file_name,
            md5_checksum=""
        )
