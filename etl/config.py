from dataclasses import dataclass, field


@dataclass
class Config:
    operation: str
    ingestion_dir: str
    incremental: bool
    selected: list[str] | None = field(default=None)
    skip: list[str] | None = field(default=None)
    backup: bool = False
