"""Collector gRPC server backed by Redis aggregates and per-sensor stats."""
import asyncio
import time
from collections import defaultdict

import grpc

from proto import telemetry_pb2
from proto import telemetry_pb2_grpc

import redis.asyncio as redis
import argparse
import json
import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

# -----------------------------
# In-memory aggregation store
# -----------------------------
class AggregationStore:
    def __init__(self):
        # key = (sensor_type, location)
        self.data = defaultdict(
            lambda: {"count": 0, "sum": 0.0, "min": float("inf"), "max": float("-inf")}
        )

    def update(self, measurement):
        key = (measurement.meta.sensor_type, measurement.meta.location)

        entry = self.data[key]
        entry["count"] += 1
        entry["sum"] += measurement.value
        entry["min"] = min(entry["min"], measurement.value)
        entry["max"] = max(entry["max"], measurement.value)

    def snapshot(self):
        return dict(self.data)


class AggregationStoreAsync:
    async def update(self, measurement):
        type_loc_key = f"agg:{measurement.meta.sensor_type}:{measurement.meta.location}"
        sensor_stats_key = f"sensor:{measurement.meta.sensor_id}:stats"
        sensor_recent_key = f"sensor:{measurement.meta.sensor_id}:recent"

        # Atomic-ish update using pipeline
        async with redis_client.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(type_loc_key, sensor_stats_key)

                     # --- GLOBAL AGGREGATION ---
                    global_current = await pipe.hgetall(type_loc_key)
                    g_count = int(global_current.get("count", 0)) + 1
                    g_sum = float(global_current.get("sum", 0)) + measurement.value
                    g_min = min(float(global_current.get("min", measurement.value)), measurement.value)
                    g_max = max(float(global_current.get("max", measurement.value)), measurement.value)

                    # --- PER SENSOR STATS ---
                    sensor_current = await pipe.hgetall(sensor_stats_key)
                    s_count = int(sensor_current.get("count", 0)) + 1
                    s_sum = float(sensor_current.get("sum", 0)) + measurement.value
                    s_min = min(float(sensor_current.get("min", measurement.value)), measurement.value)
                    s_max = max(float(sensor_current.get("max", measurement.value)), measurement.value)

                    # Prepare recent entry
                    recent_entry = json.dumps({
                        "ts": measurement.ts_unix_ms,
                        "value": measurement.value
                    })

                    pipe.multi()

                    # Update global
                    pipe.hset(type_loc_key, mapping={
                        "count": g_count,
                        "sum": g_sum,
                        "min": g_min,
                        "max": g_max,
                    })

                    # Update per-sensor stats
                    pipe.hset(sensor_stats_key, mapping={
                        "count": s_count,
                        "sum": s_sum,
                        "min": s_min,
                        "max": s_max,
                        "sensor_type": measurement.meta.sensor_type,
                        "location": measurement.meta.location,
                        "updated_unix_ms": measurement.ts_unix_ms,
                    })

                    # Update recent list (keep last 20)
                    pipe.lpush(sensor_recent_key, recent_entry)
                    pipe.ltrim(sensor_recent_key, 0, 19)

                    await pipe.execute()
                    break

                except redis.WatchError:
                    continue
    
    async def snapshot(self):
        keys = await redis_client.keys("agg:*")
        result = {}

        for key in keys:
            data = await redis_client.hgetall(key)
            result[key] = data

        return result

store = AggregationStoreAsync()


# ----------------------------------------------------------
# 1️⃣ Client-streaming ingestion service
# ----------------------------------------------------------
class IngestService(telemetry_pb2_grpc.IngestServiceServicer):

    async def PushMeasurements(self, request_iterator, context):
        received = 0

        async for measurement in request_iterator:
            await store.update(measurement)
            received += 1

            print(
                f"[Ingest] "
                f"{measurement.meta.sensor_id} "
                f"{measurement.meta.sensor_type}@{measurement.meta.location} "
                f"value={measurement.value:.2f}"
            )

        return telemetry_pb2.IngestAck(received=received)
        

        


# ----------------------------------------------------------
# 2️⃣ Server-streaming aggregate service
# ----------------------------------------------------------
class AggregateService(telemetry_pb2_grpc.AggregateServiceServicer):

    async def StreamAggregates(self, request, context):
        print("[Collector] AggregateService stream started")

        requested_keys = None
        if request.keys:
            requested_keys = {
                f"agg:{key.sensor_type}:{key.location}"
                for key in request.keys
            }

        previous_snapshot = {}
        send_initial_snapshot = request.send_initial_snapshot
        min_interval_s = max(request.min_update_interval_ms, 0) / 1000.0
        last_sent_at = {}

        while True:
            if context.cancelled():
                print("[Collector] AggregateService stream cancelled")
                return

            snapshot = await store.snapshot()

            for redis_key, data in snapshot.items():
                if requested_keys is not None and redis_key not in requested_keys:
                    continue

                normalized = {
                    "count": int(data.get("count", 0)),
                    "sum": float(data.get("sum", 0.0)),
                    "min": float(data.get("min", 0.0)),
                    "max": float(data.get("max", 0.0)),
                }

                if redis_key not in previous_snapshot:
                    previous_snapshot[redis_key] = normalized
                    if not send_initial_snapshot:
                        continue

                changed = previous_snapshot.get(redis_key) != normalized
                if not changed and not send_initial_snapshot:
                    continue

                now = time.time()
                if min_interval_s > 0:
                    last_sent = last_sent_at.get(redis_key, 0.0)
                    if changed and (now - last_sent) < min_interval_s:
                        continue

                try:
                    _, sensor_type, location = redis_key.split(":", 2)
                except ValueError:
                    continue

                previous_snapshot[redis_key] = normalized
                last_sent_at[redis_key] = now

                yield telemetry_pb2.Aggregate(
                    key=telemetry_pb2.AggregateKey(
                        sensor_type=sensor_type,
                        location=location,
                    ),
                    count=normalized["count"],
                    sum=normalized["sum"],
                    min=normalized["min"],
                    max=normalized["max"],
                    updated_unix_ms=int(time.time() * 1000),
                )

            send_initial_snapshot = False
            await asyncio.sleep(1)
                    
# ----------------------------------------------------------
# 3️⃣ Unary query service
# ----------------------------------------------------------
class QueryService(telemetry_pb2_grpc.QueryServiceServicer):

    async def GetSensorStats(self, request, context):
        sensor_id = request.sensor_id

        sensor_stats_key = f"sensor:{sensor_id}:stats"
        sensor_recent_key = f"sensor:{sensor_id}:recent"

        stats = await redis_client.hgetall(sensor_stats_key)

        if not stats:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Sensor not found")

        recent_raw = await redis_client.lrange(sensor_recent_key, 0, 19)

        recent_values = []
        for item in recent_raw:
            parsed = json.loads(item)
            recent_values.append(
                telemetry_pb2.RecentValue(
                    ts_unix_ms=int(parsed["ts"]),
                    value=float(parsed["value"]),
                )
            )

        return telemetry_pb2.GetSensorStatsResponse(
            meta=telemetry_pb2.SensorMeta(
                sensor_id=sensor_id,
                sensor_type=stats.get("sensor_type", ""),
                location=stats.get("location", ""),
            ),
            count=int(stats.get("count", 0)),
            sum=float(stats.get("sum", 0.0)),
            min=float(stats.get("min", 0.0)),
            max=float(stats.get("max", 0.0)),
            updated_unix_ms=int(stats.get("updated_unix_ms", 0)),
            recent=recent_values,
        )
        

        
        
    
            


# -----------------------------
# Periodic stats printer
# -----------------------------
async def stats_printer():
    while True:
        await asyncio.sleep(5)
        snap = await store.snapshot()

        print("\n===== REDIS AGGREGATES =====")
        for key, v in snap.items():
            if not v:
                continue
            
            count = int(v["count"])
            sum_v = float(v["sum"])
            
            avg = sum_v / count if count else 0
            print(
                f"{key} count = {count} avg={avg:.2f} "
                f"min={float(v['min']):.2f} max={float(v['max']):.2f}"
            )
        print("============================\n")


# -----------------------------
# Server bootstrap
# -----------------------------
async def serve(port: int):
    server = grpc.aio.server()

    telemetry_pb2_grpc.add_IngestServiceServicer_to_server(
        IngestService(), server
    )
    
    telemetry_pb2_grpc.add_AggregateServiceServicer_to_server(
        AggregateService(), server
    )

    telemetry_pb2_grpc.add_QueryServiceServicer_to_server(
        QueryService(), server
    )


    server.add_insecure_port(f"[::]:{port}")

    await server.start()
    print(f"Collector running on :{port}")

    asyncio.create_task(stats_printer())

    await server.wait_for_termination()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50051)
    args = ap.parse_args()
    preferred_port = args.port
    asyncio.run(serve(preferred_port))


