"""冻结批次 GraphQL 采集的端到端演示。

场景与题目一一对应：
1. expA 测量第二页首次返回 data+errors（path 指向分支），expB 先取完；
2. 第一轮停在 blocked，已取齐的分支原样保留，重启只补失败分支；
3. 全部取齐后才发布版本，人工抢先提交版本时 expected_version 冲突；
4. 行级谱系可追到实验、原始响应页与请求参数；
5. 冻结 revision 失效后旧结果仍可查看，但无法与新 revision 拼成完整数据。
"""

import tempfile
from pathlib import Path

from lab.http import serving
from lab.provider import Provider
from .service import Vault
from .harvest.client import GraphQLClient
from .validation import VaultError, canonical
from .releases.exporter import verify_directory

SCHEMA = {
    "id": "measurements",
    "version": 1,
    "fields": [
        {"name": "id", "type": "text"},
        {"name": "value", "type": "decimal", "minimum": "0"},
        {"name": "group", "type": "enum", "choices": ["A", "B"]},
    ],
    "primary_key": ["id"],
}


def demonstrate():
    report = {"steps": []}
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory) / "vault"
        provider = Provider()
        with serving(provider) as endpoint:
            with Vault(root) as vault:
                vault.registry.add_schema(SCHEMA)
                vault.registry.register(
                    "trial", "Frozen batch measurements", "measurements", "operator"
                )
                client = GraphQLClient(endpoint)

                # 第一轮：外层两页取完；expA 第二页分支失败，expB 先取完。
                # 首轮不重试失败分支，让 blocked 现场清晰可见。
                vault.harvests.open("batch-h1", endpoint, "batch1", "trial")
                first = vault.harvests.run(
                    "batch-h1", client, retry_failed_branches=False
                )
                assert first["status"] == "blocked", first
                branches = {item["id"]: item for item in first["experiments"]}
                assert branches["expA"]["error"]["path"] == ["experiment", "measurements"]
                assert branches["expB"]["complete"] is True
                assert first["outer_complete"] is True
                report["steps"].append(
                    {
                        "step": "first_run_blocked",
                        "status": first["status"],
                        "outer_complete": first["outer_complete"],
                        "expA_error": branches["expA"]["error"],
                        "expB_complete": branches["expB"]["complete"],
                        "rows_kept": first["rows"],
                        "remaining_cost": first["remaining_cost"],
                    }
                )

                # 重启：只重试缺失/失败分支，已取齐部分不会产生任何新版本。
                versions_before = len(vault.registry.list_versions("trial"))
                resumed = vault.harvests.run("batch-h1", GraphQLClient(endpoint))
                assert resumed["status"] == "complete", resumed
                assert len(vault.registry.list_versions("trial")) == versions_before
                report["steps"].append(
                    {
                        "step": "resume_completed",
                        "status": resumed["status"],
                        "requests_total": resumed["requests_made"],
                        "rows": resumed["rows"],
                        "experiments": resumed["experiments"],
                    }
                )

                # 人工期间先提交了一版（与采集并行的手工入库），head 变为 1。
                manual = (
                    canonical({"id": "manual-1", "value": "1", "group": "A"}) + "\n"
                ).encode()
                vault.ingestion.ingest(
                    "manual-ingest",
                    "trial",
                    1,
                    manual,
                    "jsonl",
                    "internal",
                    ["research"],
                    expected_version=0,
                )
                # 采集侧仍按旧 head（0）发布：expected_version 冲突必须显式报出。
                conflict = None
                try:
                    vault.harvests.publish(
                        "batch-h1",
                        "stale-publish",
                        "internal",
                        ["research"],
                        expected_version=0,
                    )
                except VaultError as exc:
                    conflict = exc.as_dict()
                assert conflict and conflict["code"] == "conflict", conflict
                report["steps"].append({"step": "expected_version_conflict", "error": conflict})

                # 用当前 head 发布，走既有 ingestion：schema 校验、artifact 摘要、版本登记。
                published = vault.harvests.publish(
                    "batch-h1", "publish-batch1", "internal", ["research"]
                )
                assert published["version"] == 2 and published["rows"] == 4
                repeated = vault.harvests.publish(
                    "batch-h1", "publish-batch1", "internal", ["research"]
                )
                assert repeated["reused"] is True
                report["steps"].append(
                    {"step": "published", "result": published, "retry_reused": repeated["reused"]}
                )

                # 行 -> 实验 -> 原页 artifact -> 请求参数，逐层可追。
                lineage = vault.harvests.lineage("batch-h1", "expA-2")
                record = lineage["records"][0]
                page_artifact = record["source_page"]["artifact"]
                envelope = vault.harvests.store.read_page("batch-h1", page_artifact)
                assert record["experiment_id"] == "expA"
                assert record["source_page"]["variables"] == {
                    "revision": "r1",
                    "experiment": "expA",
                    "after": "1",
                }
                assert envelope["request"]["variables"]["after"] == "1"
                report["steps"].append(
                    {
                        "step": "row_lineage",
                        "record": record,
                        "raw_page_operation": envelope["request"]["operationName"],
                        "raw_page_request": envelope["request"],
                    }
                )

                # 冻结 revision 失效：batch-h2 在过期前已开始采集（锁定 r1），
                # 过期后继续时检测到 extensions 里的 revision 已变成 r2，明确进入 expired。
                vault.harvests.open("batch-h2", endpoint, "batch1", "trial")
                partial = vault.harvests.run(
                    "batch-h2", GraphQLClient(endpoint), max_requests=2
                )
                assert partial["status"] == "blocked" and partial["revision"] == "r1"
                import json as _json
                import urllib.request as _urllib

                control = _urllib.Request(
                    endpoint + "/control",
                    data=_json.dumps({"expire": True}).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with _urllib.urlopen(control, timeout=3) as response:
                    _json.load(response)
                old_status = vault.harvests.status("batch-h1")
                assert old_status["status"] == "published"
                expired = vault.harvests.run("batch-h2", GraphQLClient(endpoint))
                assert expired["status"] == "expired", expired
                assert expired["blockers"][0]["code"] == "frozen_revision_expired"
                report["steps"].append(
                    {
                        "step": "frozen_revision_expired",
                        "old_harvest_viewable": old_status["status"],
                        "partial_before_expiry": {
                            "status": partial["status"],
                            "revision": partial["revision"],
                            "rows": partial["rows"],
                        },
                        "new_harvest": expired["status"],
                        "blocker": expired["blockers"][0],
                    }
                )
                try:
                    vault.harvests.run("batch-h2", GraphQLClient(endpoint))
                except VaultError as exc:
                    report["steps"].append(
                        {"step": "expired_harvest_not_resumable", "code": exc.code}
                    )
                else:
                    raise AssertionError("expired harvest must not resume")

                # 已发布版本仍可走既有 release/export 通道。
                vault.releases.plan(
                    "release",
                    [{"dataset": "trial", "version": published["version"]}],
                    "research",
                    "internal",
                )
                vault.releases.approve("release", 1, "reviewer", "Frozen batch verified")
                exported = vault.exporter.export(
                    "export", "release", Path(directory) / "release"
                )
                verified = verify_directory(Path(directory) / "release")
                report["steps"].append(
                    {
                        "step": "release_export",
                        "export": exported,
                        "verify": verified,
                    }
                )
                report["audit"] = vault.database.audit_check()
        report["canonical_note"] = "发布数据行：" + canonical(
            [
                {"id": "expA-1", "value": "10", "group": "A"},
                {"id": "expA-2", "value": "20", "group": "A"},
                {"id": "expB-1", "value": "10", "group": "B"},
                {"id": "expB-2", "value": "20", "group": "B"},
            ]
        )
    return report


def main():
    print(_printable(demonstrate()))


def _printable(value):
    import json

    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
