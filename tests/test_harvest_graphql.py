import tempfile
import unittest
import urllib.request
import json
from pathlib import Path

from lab.http import serving
from lab.provider import Provider
from trialvault.service import Vault
from trialvault.harvest.client import GraphQLClient
from trialvault.validation import VaultError, canonical
from support import schema, register


def control(base, payload):
    request = urllib.request.Request(
        base + "/control",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


class HarvestIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "vault"
        self.vault = Vault(self.root)
        self.addCleanup(self.vault.close)
        register(self.vault, dataset="trial")

    def _open(self, harvest="h1", provider=None):
        provider = provider or Provider()
        context = serving(provider)
        base = context.__enter__()
        self.addCleanup(lambda: context.__exit__(None, None, None))
        self.vault.harvests.open(harvest, base, "batch1", "trial")
        return base, provider

    def test_nested_pagination_failure_resume_and_publish(self):
        base, provider = self._open()
        # 首轮不重试失败分支：expA 第二页错误被保留，expB 先取完，整体停在 blocked。
        first = self.vault.harvests.run(
            "h1", GraphQLClient(base), retry_failed_branches=False
        )
        self.assertEqual(first["status"], "blocked")
        self.assertTrue(first["outer_complete"])
        branches = {item["id"]: item for item in first["experiments"]}
        # 外层 hasNextPage=false 没有被当成整体结束：expA 内层仍标记未完成。
        self.assertFalse(branches["expA"]["complete"])
        self.assertEqual(
            branches["expA"]["error"]["path"], ["experiment", "measurements"]
        )
        self.assertEqual(branches["expA"]["error"]["code"], "RETRY")
        # 另一个实验先取完，且成功分支在重启间原样保留。
        self.assertTrue(branches["expB"]["complete"])
        self.assertEqual(branches["expB"]["measurements"], 2)
        self.assertEqual(first["rows"], 3)

        # 重启前不存在任何数据集版本。
        self.assertEqual(self.vault.registry.list_versions("trial"), [])
        # 重启时开启失败分支重试，只补缺失/失败页。
        resumed = self.vault.harvests.run("h1", GraphQLClient(base))
        self.assertEqual(resumed["status"], "complete")
        self.assertEqual(resumed["rows"], 4)
        # 只补缺失分支：成功分支不会被重新请求。
        self.assertEqual(provider.calls[-1]["variables"]["experiment"], "expA")
        self.assertEqual(self.vault.registry.list_versions("trial"), [])

        published = self.vault.harvests.publish(
            "h1", "pub", "internal", ["research"]
        )
        self.assertEqual(published["version"], 1)
        self.assertEqual(published["rows"], 4)
        rows = self.vault.ingestion.rows("trial", 1)
        self.assertEqual(
            rows,
            [
                {"id": "expA-1", "value": "10", "group": "A"},
                {"id": "expA-2", "value": "20", "group": "A"},
                {"id": "expB-1", "value": "10", "group": "B"},
                {"id": "expB-2", "value": "20", "group": "B"},
            ],
        )
        # 发布幂等：同 request id 复用，不产生新版本。
        again = self.vault.harvests.publish("h1", "pub", "internal", ["research"])
        self.assertTrue(again["reused"])
        self.assertEqual(len(self.vault.registry.list_versions("trial")), 1)
        with self.assertRaises(VaultError):
            self.vault.harvests.publish("h1", "other", "internal", ["research"])

    def test_manual_version_triggers_expected_version_conflict(self):
        base, _ = self._open()
        self.vault.harvests.run("h1", GraphQLClient(base))
        self.vault.harvests.run("h1", GraphQLClient(base))
        # 人工期间抢先发布一版。
        from support import ingest

        ingest(self.vault, request="manual", dataset="trial", expected=0)
        with self.assertRaises(VaultError) as caught:
            self.vault.harvests.publish(
                "h1", "pub", "internal", ["research"], expected_version=0
            )
        self.assertEqual(caught.exception.code, "conflict")
        self.assertEqual(caught.exception.details["actual"], 1)
        # 显式带上当前 head 即可发布为下一版。
        published = self.vault.harvests.publish(
            "h1", "pub", "internal", ["research"], expected_version=1
        )
        self.assertEqual(published["version"], 2)

    def test_lineage_traces_row_to_page_and_request(self):
        base, _ = self._open()
        self.vault.harvests.run("h1", GraphQLClient(base))
        self.vault.harvests.run("h1", GraphQLClient(base))
        self.vault.harvests.publish("h1", "pub", "internal", ["research"])
        lineage = self.vault.harvests.lineage("h1", "expA-2")
        record = lineage["records"][0]
        self.assertEqual(record["experiment_id"], "expA")
        self.assertEqual(record["revision"], "r1")
        page = record["source_page"]
        self.assertEqual(
            page["variables"],
            {"revision": "r1", "experiment": "expA", "after": "1"},
        )
        envelope = self.vault.harvests.store.read_page("h1", page["artifact"])
        self.assertEqual(envelope["request"]["operationName"], "Measurements")
        self.assertEqual(envelope["request"]["variables"], page["variables"])
        # 失败响应也完整留痕，错误 path 与成功页同目录可审计。
        pages = self.vault.harvests.store.load("h1")["pages"]
        error_page = next(
            item for item in pages if item["error_paths"] == [["experiment", "measurements"]]
        )
        raw = self.vault.harvests.store.read_page("h1", error_page["artifact"])
        self.assertEqual(raw["errors"][0]["extensions"]["code"], "RETRY")
        self.assertIsNone(raw["data"]["experiment"]["measurements"])
        # 发布版本 metadata 指向谱系 artifact。
        version = self.vault.registry.version("trial", 1)
        self.assertEqual(
            version["payload"]["metadata"]["lineage_artifact"],
            self.vault.harvests.store.load("h1")["lineage_artifact"]["sha256"],
        )
        with self.assertRaises(VaultError):
            self.vault.harvests.lineage("h1", "missing")

    def test_budget_exhaustion_is_recoverable_and_never_gambles(self):
        provider = Provider(cost=2)
        base, _ = self._open(provider=provider)
        first = self.vault.harvests.run("h1", GraphQLClient(base))
        # OpenBatch + 一个外层页后服务端预算归零，状态保留为 blocked。
        self.assertEqual(first["status"], "blocked")
        self.assertTrue(first["budget_exhausted"])
        self.assertEqual(first["remaining_cost"], 0)
        self.assertEqual(first["requests_made"], 2)
        # 不充值直接重启：不会发出任何请求碰运气。
        again = self.vault.harvests.run("h1", GraphQLClient(base))
        self.assertEqual(again["status"], "blocked")
        self.assertEqual(again["requests_made"], 2)
        # 服务端与本地预算同时充值后继续，已落地的两页不重复。
        control(base, {"credit": 10})
        resumed = self.vault.harvests.run(
            "h1", GraphQLClient(base), budget_credit=10
        )
        self.assertEqual(resumed["status"], "complete")
        self.assertEqual(resumed["rows"], 4)

    def test_server_429_marks_budget_exhausted(self):
        # 初始只够一次成功请求，第二次由服务端以 HTTP 429 拒绝。
        provider = Provider(cost=1)
        base, _ = self._open(provider=provider)
        result = self.vault.harvests.run("h1", GraphQLClient(base))
        self.assertEqual(result["status"], "blocked")
        self.assertTrue(result["budget_exhausted"])
        self.assertEqual(result["blockers"][0]["code"], "budget_exhausted")
        # 服务端先恢复额度，再由本地显式充值放行；恢复后重新信任 extensions。
        control(base, {"credit": 20})
        completed = self.vault.harvests.run(
            "h1", GraphQLClient(base), budget_credit=20
        )
        self.assertEqual(completed["status"], "complete")
        self.assertEqual(completed["rows"], 4)

    def test_frozen_revision_expiry_blocks_reassembly(self):
        base, _ = self._open()
        # 先拿到部分数据（不重试失败分支，保留 blocked 现场）。
        first = self.vault.harvests.run(
            "h1", GraphQLClient(base), retry_failed_branches=False
        )
        self.assertEqual(first["status"], "blocked")
        control(base, {"expire": True})
        expired = self.vault.harvests.run("h1", GraphQLClient(base))
        self.assertEqual(expired["status"], "expired")
        self.assertEqual(
            expired["blockers"][0]["code"], "frozen_revision_expired"
        )
        # 旧结果仍可查看，但不能发布也不能与新 revision 拼接。
        view = self.vault.harvests.status("h1")
        self.assertEqual(view["rows"], 3)
        with self.assertRaises(VaultError) as caught:
            self.vault.harvests.run("h1", GraphQLClient(base))
        self.assertEqual(caught.exception.code, "state")
        with self.assertRaises(VaultError):
            self.vault.harvests.publish("h1", "pub", "internal", ["research"])

    def test_max_requests_bounds_each_run_and_resume_uses_saved_cursors(self):
        base, _ = self._open()
        # max_requests 只计分页请求：OpenBatch 固定开销之外，本轮只允许 1 个分页请求。
        first = self.vault.harvests.run("h1", GraphQLClient(base), max_requests=1)
        self.assertEqual(first["status"], "blocked")
        self.assertEqual(first["requests_made"], 2)
        # 外层只取了第一页，游标停在 "1"。
        self.assertEqual(first["outer_after"], "1")
        second = self.vault.harvests.run("h1", GraphQLClient(base), max_requests=2)
        self.assertTrue(second["outer_complete"])
        self.assertEqual(second["requests_made"], 4)
        # 第三轮预算只够一页：expA 内层停在第一页游标 "1"，且记录下分支错误。
        third = self.vault.harvests.run("h1", GraphQLClient(base), max_requests=1)
        self.assertEqual(third["status"], "blocked")
        state = self.vault.harvests.store.load("h1")
        # 游标各管各的：外层页尾游标停在 "2"，expA 内层游标停在 "1"。
        self.assertEqual(state["outer"]["end_cursor"], "2")
        self.assertEqual(state["experiments"]["expA"]["after"], "1")
        # 外层游标绝不与内层游标混用。
        self.assertNotIn("2", state["experiments"]["expA"]["seen_cursors"])
        # 放开预算后从保存的内层游标补齐，不重取外层与已完成页。
        final = self.vault.harvests.run("h1", GraphQLClient(base))
        self.assertEqual(final["status"], "complete")
        self.assertEqual(final["rows"], 4)

    def test_open_is_idempotent_and_identity_is_immutable(self):
        base, _ = self._open()
        again = self.vault.harvests.open("h1", base, "batch1", "trial")
        self.assertTrue(again["reused"])
        with self.assertRaises(VaultError):
            self.vault.harvests.open("h1", base, "other-batch", "trial")

    def test_state_digest_tampering_is_detected(self):
        base, _ = self._open()
        self.vault.harvests.run("h1", GraphQLClient(base), max_requests=1)
        state_path = self.vault.harvests.store.state_path("h1")
        state = json.loads(state_path.read_text())
        state["revision"] = "forged"
        state_path.write_text(json.dumps(state))
        with self.assertRaises(VaultError) as caught:
            self.vault.harvests.status("h1")
        self.assertEqual(caught.exception.code, "integrity")


class FakeClient:
    """可编程 GraphQL 客户端，用于游标循环、未知错误、内容冲突等单元场景。"""

    def __init__(self, scripts):
        self.scripts = list(scripts)
        self.calls = []

    def post(self, operation, variables):
        self.calls.append((operation, variables))
        if not self.scripts:
            raise AssertionError("unexpected extra request")
        return self.scripts.pop(0)


def envelope(data=None, errors=None, remaining=99, revision="r1"):
    return {
        "http_status": 200,
        "data": data,
        "errors": errors or [],
        "extensions": {"remainingCost": remaining, "revision": revision},
        "request": {"operationName": "x", "query": "q", "variables": {}},
    }


class HarvestGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.vault = Vault(Path(self.temp.name) / "vault")
        self.addCleanup(self.vault.close)
        register(self.vault, dataset="trial")
        self.base = "http://example.invalid"
        self.vault.harvests.open("g", self.base, "batch1", "trial")

    def _run(self, scripts, retry_failed_branches=False):
        return self.vault.harvests.run(
            "g",
            FakeClient(scripts),
            retry_failed_branches=retry_failed_branches,
        )

    def _open_batch(self, remaining=99):
        return envelope(
            {"batch": {"id": "batch1", "revision": "r1"}}, remaining=remaining
        )

    def test_outer_cursor_cycle_aborts(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [{"cursor": "1", "node": {"id": "expA"}}],
                        "pageInfo": {"endCursor": "1", "hasNextPage": True},
                    }
                }
            ),
            envelope(
                {
                    "experiments": {
                        "edges": [{"cursor": "1", "node": {"id": "expA"}}],
                        "pageInfo": {"endCursor": "1", "hasNextPage": True},
                    }
                }
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(result["blockers"][0]["code"], "pagination")

    def test_outer_hasnext_without_cursor_aborts_instead_of_empty_page(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [],
                        "pageInfo": {"endCursor": None, "hasNextPage": True},
                    }
                }
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(result["blockers"][0]["code"], "pagination")

    def test_unscoped_error_aborts_and_is_not_treated_as_empty(self):
        scripts = [
            self._open_batch(),
            envelope(
                data={"experiments": None},
                errors=[{"message": "整个视图不可读", "path": ["viewer"]}],
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(result["blockers"][0]["code"], "graphql")
        self.assertEqual(self.vault.harvests.store.load("g")["experiments"], {})

    def test_inner_cursor_cycle_aborts_branch(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [{"cursor": "1", "node": {"id": "expA"}}],
                        "pageInfo": {"endCursor": "1", "hasNextPage": False},
                    }
                }
            ),
            envelope(
                {
                    "experiment": {
                        "id": "expA",
                        "measurements": {
                            "edges": [
                                {"cursor": "1", "node": {"id": "m1", "value": "1", "group": "A"}}
                            ],
                            "pageInfo": {"endCursor": "1", "hasNextPage": True},
                        },
                    }
                }
            ),
            envelope(
                {
                    "experiment": {
                        "id": "expA",
                        "measurements": {
                            "edges": [
                                {"cursor": "1", "node": {"id": "m1", "value": "1", "group": "A"}}
                            ],
                            "pageInfo": {"endCursor": "1", "hasNextPage": True},
                        },
                    }
                }
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(result["blockers"][0]["code"], "pagination")
        self.assertEqual(
            result["blockers"][0]["details"]["experiment"], "expA"
        )

    def test_reappearing_id_with_different_content_aborts(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [{"cursor": "1", "node": {"id": "expA"}}],
                        "pageInfo": {"endCursor": "1", "hasNextPage": False},
                    }
                }
            ),
            envelope(
                {
                    "experiment": {
                        "id": "expA",
                        "measurements": {
                            "edges": [
                                {"cursor": "1", "node": {"id": "m1", "value": "1", "group": "A"}}
                            ],
                            "pageInfo": {"endCursor": "1", "hasNextPage": True},
                        },
                    }
                }
            ),
            envelope(
                {
                    "experiment": {
                        "id": "expA",
                        "measurements": {
                            "edges": [
                                {"cursor": "2", "node": {"id": "m1", "value": "9", "group": "A"}}
                            ],
                            "pageInfo": {"endCursor": "2", "hasNextPage": False},
                        },
                    }
                }
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(result["blockers"][0]["code"], "content_conflict")

    def test_duplicate_id_with_same_content_is_accepted_once(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [{"cursor": "1", "node": {"id": "expA"}}],
                        "pageInfo": {"endCursor": "1", "hasNextPage": False},
                    }
                }
            ),
            envelope(
                {
                    "experiment": {
                        "id": "expA",
                        "measurements": {
                            "edges": [
                                {"cursor": "1", "node": {"id": "m1", "value": "1", "group": "A"}}
                            ],
                            "pageInfo": {"endCursor": "1", "hasNextPage": True},
                        },
                    }
                }
            ),
            envelope(
                {
                    "experiment": {
                        "id": "expA",
                        "measurements": {
                            "edges": [
                                {"cursor": "2", "node": {"id": "m1", "value": "1", "group": "A"}}
                            ],
                            "pageInfo": {"endCursor": "2", "hasNextPage": False},
                        },
                    }
                }
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["rows"], 1)

    def test_branch_missing_without_error_aborts(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [{"cursor": "1", "node": {"id": "expA"}}],
                        "pageInfo": {"endCursor": "1", "hasNextPage": False},
                    }
                }
            ),
            envelope({"experiment": {"id": "expA", "measurements": None}}),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(result["blockers"][0]["code"], "protocol")

    def test_revision_switch_in_extensions_expires_harvest(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [{"cursor": "1", "node": {"id": "expA"}}],
                        "pageInfo": {"endCursor": "1", "hasNextPage": False},
                    }
                },
                revision="r2",
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "expired")

    def test_other_branch_continues_after_scoped_branch_error(self):
        scripts = [
            self._open_batch(),
            envelope(
                {
                    "experiments": {
                        "edges": [
                            {"cursor": "1", "node": {"id": "expA"}},
                            {"cursor": "2", "node": {"id": "expB"}},
                        ],
                        "pageInfo": {"endCursor": "2", "hasNextPage": False},
                    }
                }
            ),
            envelope(
                data={"experiment": {"id": "expA", "measurements": None}},
                errors=[
                    {
                        "message": "分支不可读",
                        "path": ["experiment", "measurements"],
                        "extensions": {"code": "RETRY"},
                    }
                ],
            ),
            # expB 不受 expA 分支错误影响，同一轮继续取完。
            envelope(
                {
                    "experiment": {
                        "id": "expB",
                        "measurements": {
                            "edges": [
                                {"cursor": "1", "node": {"id": "m-b", "value": "2", "group": "B"}}
                            ],
                            "pageInfo": {"endCursor": "1", "hasNextPage": False},
                        },
                    }
                }
            ),
        ]
        result = self._run(scripts)
        self.assertEqual(result["status"], "blocked")
        branches = {item["id"]: item for item in result["experiments"]}
        self.assertFalse(branches["expA"]["complete"])
        self.assertEqual(branches["expA"]["error"]["code"], "RETRY")
        self.assertTrue(branches["expB"]["complete"])


if __name__ == "__main__":
    unittest.main()
