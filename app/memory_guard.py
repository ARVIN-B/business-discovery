from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class MemorySnapshot:
    host_available_mb: int | None
    cgroup_current_mb: int | None
    cgroup_limit_mb: int | None
    budget_mb: int
    available_floor_mb: int
    under_pressure: bool
    reason: str | None

    def as_dict(self) -> dict:
        return asdict(self)


class MemoryGuard:
    """Soft Linux memory guard; it reports pressure but cannot cap other services."""

    def __init__(
        self,
        budget_mb: int = 7168,
        available_floor_mb: int = 1024,
        *,
        meminfo_path: str = "/proc/meminfo",
        cgroup_root: str = "/sys/fs/cgroup",
    ):
        self.budget_mb = max(256, int(budget_mb))
        self.available_floor_mb = max(128, int(available_floor_mb))
        self.meminfo_path = Path(meminfo_path)
        self.cgroup_root = Path(cgroup_root)

    def snapshot(self) -> MemorySnapshot:
        host_available = self._host_available_mb()
        cgroup_current, cgroup_limit = self._cgroup_memory_mb()
        reasons = []
        if host_available is not None and host_available < self.available_floor_mb:
            reasons.append(f"host available memory {host_available} MiB is below {self.available_floor_mb} MiB")
        effective_budget = min(self.budget_mb, cgroup_limit) if cgroup_limit is not None else self.budget_mb
        if cgroup_current is not None and cgroup_current > effective_budget:
            reasons.append(f"cgroup usage {cgroup_current} MiB exceeds {effective_budget} MiB budget")
        if cgroup_current is not None and cgroup_limit is not None:
            cgroup_available = max(0, cgroup_limit - cgroup_current)
            if cgroup_available < self.available_floor_mb:
                reasons.append(f"cgroup available memory {cgroup_available} MiB is below {self.available_floor_mb} MiB")
        return MemorySnapshot(
            host_available_mb=host_available,
            cgroup_current_mb=cgroup_current,
            cgroup_limit_mb=cgroup_limit,
            budget_mb=self.budget_mb,
            available_floor_mb=self.available_floor_mb,
            under_pressure=bool(reasons),
            reason="; ".join(reasons) or None,
        )

    def _host_available_mb(self) -> int | None:
        try:
            for line in self.meminfo_path.read_text().splitlines():
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
        except (OSError, ValueError, IndexError):
            pass
        return None

    def _cgroup_memory_mb(self) -> tuple[int | None, int | None]:
        current_path = self.cgroup_root / "memory.current"
        limit_path = self.cgroup_root / "memory.max"
        try:
            current = int(current_path.read_text().strip()) // (1024 * 1024)
            raw_limit = limit_path.read_text().strip()
            limit = None if raw_limit == "max" else int(raw_limit) // (1024 * 1024)
            return current, limit
        except (OSError, ValueError):
            pass

        v1_root = self.cgroup_root / "memory"
        try:
            current = int((v1_root / "memory.usage_in_bytes").read_text().strip()) // (1024 * 1024)
            raw_limit = int((v1_root / "memory.limit_in_bytes").read_text().strip())
            # The v1 unlimited sentinel is usually close to 2**63.
            limit = None if raw_limit >= 2**60 else raw_limit // (1024 * 1024)
            return current, limit
        except (OSError, ValueError):
            return None, None
