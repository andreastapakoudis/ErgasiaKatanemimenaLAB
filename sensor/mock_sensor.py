import asyncio
import random
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class SensorReading:
    sensor_id: str
    sensor_type: str
    location: str
    seq: int
    ts_unix_ms: int
    value: float


class MockSensor:
    """Generate timestamped readings at a fixed interval."""

    def __init__(
        self,
        sensor_id: str,
        sensor_type: str,
        location: str,
        interval: float = 1.0,
        generator: Optional[Callable[[], float]] = None,
    ):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.location = location
        self.interval = interval
        self.generator = generator or self._default_generator
        self._seq = 0

    async def stream(self):
        while True:
            self._seq += 1

            yield SensorReading(
                sensor_id=self.sensor_id,
                sensor_type=self.sensor_type,
                location=self.location,
                seq=self._seq,
                ts_unix_ms=int(time.time() * 1000),
                value=self.generator(),
            )
            await asyncio.sleep(self.interval)

    def _default_generator(self) -> float:
        return random.uniform(0, 100)
