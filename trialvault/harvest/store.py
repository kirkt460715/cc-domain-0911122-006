import os
import tempfile
from pathlib import Path

from ..validation import canonical, digest, name, parse_json, reject


class HarvestStore:
    """采集任务的磁盘状态与原始页/谱系文件，全部按任务目录隔离并可恢复。"""

    def __init__(self, root):
        self.root = Path(root)
        if self.root.is_symlink():
            reject("path", "harvest root cannot be symbolic link")
        self.root.mkdir(parents=True, exist_ok=True)

    def directory(self, harvest_id):
        name(harvest_id)
        directory = self.root / harvest_id
        if directory.is_symlink():
            reject("path", "harvest directory cannot be symbolic link")
        return directory

    def state_path(self, harvest_id):
        return self.directory(harvest_id) / "state.json"

    def pages_dir(self, harvest_id):
        path = self.directory(harvest_id) / "pages"
        if path.exists() and path.is_symlink():
            reject("path", "page directory cannot be symbolic link")
        return path

    def exists(self, harvest_id):
        return self.state_path(harvest_id).is_file()

    def load(self, harvest_id):
        path = self.state_path(harvest_id)
        if not path.is_file():
            reject("missing", "harvest not found", harvest=harvest_id)
        state = parse_json(path.read_bytes())
        if not isinstance(state, dict) or "state_digest" not in state:
            reject("integrity", "harvest state envelope invalid")
        checksum = state.pop("state_digest")
        if digest(state) != checksum:
            reject("integrity", "harvest state digest mismatch", harvest=harvest_id)
        return state

    def save(self, state):
        harvest_id = name(state["harvest_id"])
        directory = self.directory(harvest_id)
        directory.mkdir(parents=True, exist_ok=True)
        self.pages_dir(harvest_id).mkdir(exist_ok=True)
        envelope = dict(state)
        envelope["state_digest"] = digest(state)
        payload = (canonical(envelope) + "\n").encode()
        fd, temporary = tempfile.mkstemp(prefix="state-", dir=directory)
        try:
            with os.fdopen(fd, "wb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.state_path(harvest_id))
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def write_page(self, harvest_id, record):
        """保存一次原始响应并返回文件 sha256；重复内容复用既有文件。"""
        pages = self.pages_dir(harvest_id)
        pages.mkdir(exist_ok=True)
        checksum = digest(record)
        path = pages / (checksum + ".json")
        if path.exists():
            if path.is_symlink() or parse_json(path.read_bytes()) != record:
                reject("integrity", "harvest page content collides", sha256=checksum)
            return checksum
        fd, temporary = tempfile.mkstemp(prefix="page-", dir=pages)
        try:
            with os.fdopen(fd, "wb") as output:
                output.write((canonical(record) + "\n").encode())
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return checksum

    def read_page(self, harvest_id, checksum):
        path = self.pages_dir(harvest_id) / (checksum + ".json")
        if path.is_symlink() or not path.is_file():
            reject("missing", "harvest page unavailable", sha256=checksum)
        record = parse_json(path.read_bytes())
        if digest(record) != checksum:
            reject("integrity", "harvest page digest mismatch", sha256=checksum)
        return record

    def write_lineage(self, harvest_id, records):
        directory = self.directory(harvest_id)
        content = (
            "\n".join(canonical(record) for record in records)
            + ("\n" if records else "")
        ).encode()
        checksum = digest(records)
        path = directory / "lineage.jsonl"
        if path.exists():
            if path.is_symlink() or path.read_bytes() != content:
                reject("integrity", "harvest lineage is immutable once written")
        else:
            fd, temporary = tempfile.mkstemp(prefix="lineage-", dir=directory)
            try:
                with os.fdopen(fd, "wb") as output:
                    output.write(content)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temporary, path)
            finally:
                Path(temporary).unlink(missing_ok=True)
        return {"sha256": checksum, "bytes": len(content), "records": len(records)}

    def list_harvests(self):
        if not self.root.is_dir():
            return []
        result = []
        for directory in sorted(self.root.iterdir()):
            if directory.is_symlink() or not directory.is_dir():
                reject("integrity", "invalid harvest partition")
            if (directory / "state.json").is_file():
                result.append(directory.name)
        return result
