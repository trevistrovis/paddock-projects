import asyncio
import threading
from typing import Optional
from app.config import MAX_QUEUE_SIZE, MAX_CONCURRENT_SAM_SCRAPES

class RequestQueue:
    _instance: Optional['RequestQueue'] = None
    _queue: asyncio.Queue
    _sam_semaphore: threading.Semaphore
    _worker_task: Optional[asyncio.Task] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
            cls._instance._sam_semaphore = threading.Semaphore(MAX_CONCURRENT_SAM_SCRAPES)
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'RequestQueue':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def enqueue(self, item_id: int) -> bool:
        """Add an item to the queue. Returns True if successful, False if queue is full."""
        try:
            await asyncio.wait_for(self._queue.put(item_id), timeout=5.0)
            print(f"[QUEUE] Enqueued item {item_id}. Queue size: {self._queue.qsize()}")
            return True
        except asyncio.TimeoutError:
            print(f"[QUEUE] Failed to enqueue item {item_id}: queue is full")
            return False

    async def dequeue(self) -> Optional[int]:
        """Get the next item from the queue."""
        try:
            item_id = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            print(f"[QUEUE] Dequeued item {item_id}. Queue size: {self._queue.qsize()}")
            return item_id
        except asyncio.TimeoutError:
            return None

    def get_sam_semaphore(self) -> threading.Semaphore:
        """Get the SAM.gov scraping semaphore for use in sync functions."""
        return self._sam_semaphore

    def queue_size(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()

    async def start_worker(self, process_func):
        """Start the background worker that processes the queue."""
        if self._worker_task is not None and not self._worker_task.done():
            print("[QUEUE] Worker already running")
            return

        async def worker():
            print("[QUEUE] Worker started")
            while True:
                item_id = await self.dequeue()
                if item_id is None:
                    await asyncio.sleep(0.5)
                    continue

                try:
                    await process_func(item_id)
                except Exception as exc:
                    print(f"[QUEUE][ERROR] Worker failed to process item {item_id}: {repr(exc)}")
                finally:
                    self._queue.task_done()

        self._worker_task = asyncio.create_task(worker())
        print("[QUEUE] Worker task created")

    async def stop_worker(self):
        """Stop the background worker."""
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
            print("[QUEUE] Worker stopped")
