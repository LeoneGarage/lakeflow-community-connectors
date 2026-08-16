#!/usr/bin/env python3
"""Analytical S3 request-cost model (Option D) for the connection-slot volume.

Predicts S3 metadata requests/hour from the slot algorithm's deterministic
cadences and the run parameters. This is an UPPER BOUND on actual S3 calls: the
volume is FUSE-mounted, so some listdir/stat calls are served from FUSE cache
and never reach S3. Comparing this to the empirical figure from
/tmp/s3_slot_cost.py tells you how much FUSE is absorbing.

Constants mirror informix.py (verified):
  slot pool                 = max.concurrent.connections (default 16)
  heartbeat interval        = 30 s   (per held slot)
  triggered sweep interval  = 0.2-0.5 s  (mean 0.35)  [full-block reads]
  continuous sweep interval = 1.0-5.0 s  (mean 3.0)   [yielding stream reads]

Per-operation S3 request accounting (each is one metadata request against S3,
modulo FUSE caching):
  sweep, slot free      -> 1 mkdir (PUT/dir-marker)                      ... success
  sweep, slot occupied  -> 1 mkdir(EEXIST) + reclaim check:
                             listdir (LIST) + stat (HEAD) ~ 2 more       ... miss/slot
  heartbeat tick        -> open(dir) + create pulse (PUT) + cleanup old
                            pulse (DELETE) + stat ~ 3 ops
  clean release         -> pulse cleanup + create released (PUT) +
                            unlink token (DELETE) + listdir + stat +
                            unlink released (DELETE) + rmdir (DELETE) ~ 6 ops
"""

from __future__ import annotations
import argparse

# --- pricing (us-east-1 S3 Standard, per request) ---
PRICE_PUT_LIST = 0.005 / 1000
PRICE_GET_OTHER = 0.0004 / 1000
PRICE_DELETE = 0.0

TRIGGERED_SWEEP_MEAN = (0.2 + 0.5) / 2
CONTINUOUS_SWEEP_MEAN = (1.0 + 5.0) / 2
HEARTBEAT_S = 30.0


def model(mode: str, flows: int, slots: int, hold_s: float, hour_s: float = 3600.0):
    """Return (requests_by_class, total_cost) for one hour.

    flows   : number of concurrently-active reader flows contending
    slots   : max.concurrent.connections
    hold_s  : mean seconds a reader holds a slot once acquired (measured ~1-5s)
    """
    sweep_mean = TRIGGERED_SWEEP_MEAN if mode == "triggered" else CONTINUOUS_SWEEP_MEAN

    # Waiters = flows that want a slot but cannot have one right now.
    waiters = max(flows - slots, 0)
    held = min(flows, slots)

    # --- Sweeps ---
    # Each waiter sweeps every sweep_mean seconds. Each sweep: for the bands it
    # visits it issues mkdir per slot until one succeeds or all fail. Model the
    # dominant cost: a waiting sweep probes ~ (slots / attempts_to_win) slots.
    # Upper-bound conservatively: a waiter that misses probes a full band of
    # slots (~slots/4 preference bands) before giving up that sweep; a waiter
    # that wins probes ~1. Use slots/4 as the per-sweep probe count for waiters.
    probes_per_sweep = max(1, slots // 4)
    sweeps_per_waiter = hour_s / sweep_mean
    # Each probe on an occupied slot: mkdir(EEXIST)=1 PUT-class attempt +
    # reclaim listdir(LIST) + stat(HEAD) = ~1 PUT + 1 LIST + 1 GET.
    waiter_puts = waiters * sweeps_per_waiter * probes_per_sweep * 1
    waiter_lists = waiters * sweeps_per_waiter * probes_per_sweep * 1
    waiter_gets = waiters * sweeps_per_waiter * probes_per_sweep * 1

    # --- Acquisitions / releases ---
    # A held slot cycles every ~hold_s (acquire, work, release). Number of
    # acquire/release cycles/hour across the pool:
    cycles = held * (hour_s / hold_s)
    # Winning mkdir = 1 PUT; release ~ 2 PUT-class (released marker) +
    # ~3 DELETE (token, released, rmdir) + 1 LIST + 1 GET(stat).
    acquire_puts = cycles * 1
    release_puts = cycles * 1  # released marker
    release_lists = cycles * 1
    release_gets = cycles * 1
    release_deletes = cycles * 3

    # --- Heartbeats ---
    # Each held slot ticks every 30s: ~1 PUT (pulse) + 1 DELETE (old pulse) +
    # 1 GET (stat). Held count ~ constant over the hour.
    ticks = held * (hour_s / HEARTBEAT_S)
    hb_puts = ticks * 1
    hb_gets = ticks * 1
    hb_deletes = ticks * 1

    put_list = waiter_puts + waiter_lists + acquire_puts + release_puts + release_lists + hb_puts
    get = waiter_gets + release_gets + hb_gets
    delete = release_deletes + hb_deletes
    total = put_list + get + delete

    cost = put_list * PRICE_PUT_LIST + get * PRICE_GET_OTHER + delete * PRICE_DELETE
    return (
        {
            "PUT/LIST (billable)": int(put_list),
            "GET/HEAD (billable)": int(get),
            "DELETE (free)": int(delete),
            "TOTAL requests": int(total),
        },
        cost,
        {
            "waiters": waiters,
            "held": held,
            "sweep_mean": sweep_mean,
            "cycles": int(cycles),
            "ticks": int(ticks),
        },
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["triggered", "continuous"], default="triggered")
    ap.add_argument("--flows", type=int, default=60)
    ap.add_argument("--slots", type=int, default=16)
    ap.add_argument("--hold", type=float, default=3.0, help="mean slot hold seconds")
    args = ap.parse_args()

    rq, cost, ctx = model(args.mode, args.flows, args.slots, args.hold)
    print(f"Model: mode={args.mode} flows={args.flows} slots={args.slots} " f"hold={args.hold}s")
    print(
        f"  waiters={ctx['waiters']} held={ctx['held']} "
        f"sweep_mean={ctx['sweep_mean']}s cycles/h={ctx['cycles']:,} "
        f"heartbeat_ticks/h={ctx['ticks']:,}"
    )
    print(f"\n  {'class':<24}{'req/hour':>14}")
    print("  " + "-" * 38)
    for k, v in rq.items():
        print(f"  {k:<24}{v:>14,}")
    print(f"\n  Cost/hour:  ${cost:.4f}")
    print(f"  Cost/day:   ${cost*24:.4f}")
    print(f"  Cost/30d:   ${cost*24*30:.2f}")
    print("\n  (UPPER BOUND: FUSE cache serves some LIST/HEAD without hitting S3.)")

    # Show the triggered-vs-continuous sweep sensitivity, since the deployed
    # triggered sweep tightening (1-5s -> 0.2-0.5s) is ~10x on waiter sweeps.
    if args.mode == "triggered":
        _, cost_cont, _ = model("continuous", args.flows, args.slots, args.hold)
        print(f"\n  For comparison, same params at continuous sweep (1-5s): " f"${cost_cont:.4f}/h")
        print(f"  Triggered tightening cost multiplier: {cost/cost_cont:.1f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
