from ..validation import (
    canonical,
    digest,
    integer,
    name,
    reject,
)
from .client import HarvestTransportError
from .store import HarvestStore

# 状态机：opened -> collecting -> complete / blocked / aborted / expired
#          complete -> published；blocked 可 resume；aborted/expired 不可恢复。
TERMINAL = {"aborted", "expired", "published"}


class HarvestManager:
    def __init__(self, database, registry, ingestion, artifacts, root):
        self.db = database
        self.registry = registry
        self.ingestion = ingestion
        self.artifacts = artifacts
        self.store = HarvestStore(root)

    # ------------------------------------------------------------------ 打开
    def open(self, harvest_id, endpoint, batch_id, dataset_id, schema_version=1):
        name(harvest_id)
        if not isinstance(endpoint, str) or not endpoint.startswith(("http://", "https://")):
            reject("input", "endpoint must be an http(s) URL")
        name(batch_id)
        # 数据集与 schema 必须已登记；采集期间只攒数据，发布时才产生版本。
        dataset = self.registry.dataset(dataset_id)
        self.registry.schema(dataset["schema_id"], integer(schema_version, 1))
        if self.store.exists(harvest_id):
            state = self.store.load(harvest_id)
            identity = {
                "endpoint": state["endpoint"],
                "batch_id": state["batch_id"],
                "dataset": state["dataset"],
                "schema_version": state["schema_version"],
            }
            expected = {
                "endpoint": endpoint,
                "batch_id": batch_id,
                "dataset": dataset_id,
                "schema_version": schema_version,
            }
            if identity != expected:
                reject("conflict", "harvest identity differs", harvest=harvest_id)
            return self._summary(state, reused=True)
        state = {
            "harvest_id": harvest_id,
            "endpoint": endpoint,
            "batch_id": batch_id,
            "dataset": dataset_id,
            "schema_version": schema_version,
            "status": "opened",
            "revision": None,
            "outer": {
                "after": None,
                "end_cursor": None,
                "has_next_page": None,
                "complete": False,
                "seen_cursors": [],
            },
            "experiments": {},
            "row_order": [],
            "rows": {},
            "pages": [],
            "requests_made": 0,
            "server_remaining_cost": None,
            "server_budget_stale": False,
            "local_credit": None,
            "server_rejected_budget": False,
            "budget_exhausted": False,
            "frozen_expired": None,
            "blockers": [],
            "lineage_artifact": None,
            "publication": None,
        }
        self.store.save(state)
        with self.db.transaction():
            self.db.audit("harvest_opened", harvest_id, {"batch": batch_id, "dataset": dataset_id})
        return self._summary(state, reused=False)

    # ------------------------------------------------------------------ 运行
    def run(
        self,
        harvest_id,
        client,
        max_requests=None,
        budget_credit=0,
        retry_failed_branches=True,
    ):
        state = self.store.load(harvest_id)
        if state["status"] in TERMINAL:
            reject("state", "harvest cannot run in terminal state", status=state["status"])
        if not isinstance(client, object) or not hasattr(client, "post"):
            reject("input", "graphql client required")
        if max_requests is not None:
            integer(max_requests, 1, 100000)
        integer(budget_credit, 0, 10**12)
        if not isinstance(retry_failed_branches, bool):
            reject("input", "retry_failed_branches must be boolean")
        state["retry_failed_branches"] = retry_failed_branches
        if budget_credit:
            # 本地充值是新预算事实：服务端旧额度可能已过期，待下一次成功响应刷新。
            state["local_credit"] = (state.get("local_credit") or 0) + budget_credit
            state["server_budget_stale"] = True
            state["server_rejected_budget"] = False
            state["budget_exhausted"] = False
            self.store.save(state)
        state["status"] = "collecting"
        state["blockers"] = []
        state["frozen_expired"] = None
        state["max_branch_retries"] = 3
        # 充值可能已解除预算约束：按当前有效额度重算耗尽标志，避免上一轮的 stale 状态误判。
        effective = self._effective_budget(state)
        state["budget_exhausted"] = effective is not None and effective <= 0
        # 每轮重新打开错误分支：历史错误保留在 pages/experiments 里，当前轮次允许再试。
        state["retry_phase"] = False
        for branch in state["experiments"].values():
            branch["error"] = None
            branch["retries_this_run"] = 0
        # max_requests 只约束分页请求；OpenBatch 是打开冻结批次的固定开销，单独计数。
        paged = 0

        def quota_reached():
            return max_requests is not None and paged >= max_requests

        def finish(status):
            state["status"] = status
            self.store.save(state)
            return self._summary(state)

        def abort():
            # blockers 已在各分支函数中按 {code,message,details} 写好，原样保留上下文。
            state["status"] = "aborted"
            self.store.save(state)
            with self.db.transaction():
                self.db.audit("harvest_aborted", harvest_id, state["blockers"])
            return self._summary(state)

        def expired(message, **details):
            state["status"] = "expired"
            state["frozen_expired"] = {
                "code": "frozen_revision_expired",
                "message": message,
                "details": details,
            }
            state["blockers"] = [state["frozen_expired"]]
            self.store.save(state)
            with self.db.transaction():
                self.db.audit("harvest_expired", harvest_id, state["blockers"])
            return self._summary(state)

        # 第一步：打开冻结批次，锁定 revision。
        if state["revision"] is None:
            response = self._request(state, client, "OpenBatch", {})
            if response is None:
                if state.get("frozen_expired"):
                    return expired("frozen revision expired while opening batch")
                return finish("blocked")
            data = response["data"] or {}
            batch = data.get("batch")
            if not isinstance(batch, dict) or not isinstance(batch.get("revision"), str):
                state["blockers"] = [
                    {
                        "code": "protocol",
                        "message": "OpenBatch did not return a frozen revision",
                        "details": {},
                    }
                ]
                return abort()
            scoped = self._classify_errors(response)
            if scoped["unscoped"]:
                state["blockers"] = [
                    {
                        "code": "graphql",
                        "message": "unscoped error while opening batch",
                        "details": {"errors": scoped["unscoped"]},
                    }
                ]
                return abort()
            state["revision"] = batch["revision"]
            state["batch_id"] = batch.get("id", state["batch_id"])
            self.store.save(state)

        while True:
            if quota_reached():
                return finish("blocked")
            # 外层还没取完：继续外层页。
            if not state["outer"]["complete"]:
                outcome = self._fetch_outer(state, client)
                if outcome == "request_blocked":
                    if state.get("frozen_expired"):
                        return expired("frozen revision expired while paging experiments")
                    return finish("blocked")
                if outcome == "expired":
                    return expired("frozen revision expired while paging experiments")
                if outcome == "aborted":
                    return abort()
                paged += 1
                self.store.save(state)
                continue
            # 外层取完后处理内层分支。
            branch_id = self._next_open_branch(state)
            if branch_id is None:
                break
            outcome = self._fetch_inner(state, client, branch_id)
            if outcome == "request_blocked":
                if state.get("frozen_expired"):
                    return expired(
                        "frozen revision expired while paging measurements",
                        experiment=branch_id,
                    )
                return finish("blocked")
            if outcome == "expired":
                return expired("frozen revision expired while paging measurements", experiment=branch_id)
            if outcome == "aborted":
                return abort()
            paged += 1
            self.store.save(state)

        if self._has_open_branch(state):
            return finish("blocked")
        state["status"] = "complete"
        self.store.save(state)
        with self.db.transaction():
            self.db.audit(
                "harvest_completed",
                harvest_id,
                {
                    "revision": state["revision"],
                    "experiments": len(state["experiments"]),
                    "rows": len(state["row_order"]),
                    "pages": len(state["pages"]),
                },
            )
        return self._summary(state)

    # ------------------------------------------------------------------ 外层
    # ------------------------------------------------------------------ 外层
    def _fetch_outer(self, state, client):
        variables = {"revision": state["revision"], "after": state["outer"]["after"]}
        response = self._request(state, client, "Experiments", variables)
        if response is None:
            return "request_blocked"
        scoped = self._classify_errors(response)
        if scoped["revision_expired"]:
            return "expired"
        if scoped["unscoped"]:
            self._block(
                state,
                "graphql",
                "unscoped experiment-page error",
                errors=scoped["unscoped"],
            )
            return "aborted"
        data = response.get("data")
        page = data.get("experiments") if isinstance(data, dict) else None
        # 未知范围的错误不能被当作空数组：缺页结构必须中止。
        if not isinstance(page, dict) or not isinstance(page.get("pageInfo"), dict):
            self._block(state, "protocol", "experiments page structure missing")
            return "aborted"
        edges = page.get("edges")
        page_info = page["pageInfo"]
        if not isinstance(edges, list) or not isinstance(page_info.get("hasNextPage"), bool):
            self._block(state, "protocol", "experiments page malformed")
            return "aborted"
        end_cursor = page_info.get("endCursor")
        if end_cursor is not None and not isinstance(end_cursor, str):
            self._block(state, "protocol", "endCursor must be text or null")
            return "aborted"
        for edge in edges:
            if not isinstance(edge, dict) or not isinstance(edge.get("node"), dict):
                self._block(state, "protocol", "experiment edge malformed")
                return "aborted"
            experiment_id = edge["node"].get("id")
            if not isinstance(experiment_id, str) or not experiment_id:
                self._block(state, "protocol", "experiment id missing")
                return "aborted"
            if experiment_id not in state["experiments"]:
                state["experiments"][experiment_id] = self._new_branch(experiment_id, edge.get("cursor"))
        has_next = page_info["hasNextPage"]
        if has_next:
            # 页尾游标无进展或循环要明确中止，绝不用空页冒充结束。
            if end_cursor is None:
                self._block(state, "pagination", "hasNextPage without endCursor")
                return "aborted"
            if end_cursor in state["outer"]["seen_cursors"] or end_cursor == state["outer"]["after"]:
                self._block(
                    state,
                    "pagination",
                    "outer cursor stalled or cycling",
                    cursor=end_cursor,
                )
                return "aborted"
            state["outer"]["seen_cursors"].append(end_cursor)
            state["outer"]["after"] = end_cursor
        else:
            state["outer"]["complete"] = True
        state["outer"]["end_cursor"] = end_cursor
        state["outer"]["has_next_page"] = has_next
        return "ok"

    # ------------------------------------------------------------------ 内层
    def _fetch_inner(self, state, client, experiment_id):
        branch = state["experiments"][experiment_id]
        variables = {
            "revision": state["revision"],
            "experiment": experiment_id,
            "after": branch["after"],
        }
        response = self._request(state, client, "Measurements", variables)
        if response is None:
            return "request_blocked"
        scoped = self._classify_errors(response, experiment_id)
        if scoped["revision_expired"]:
            return "expired"
        if scoped["unscoped"]:
            self._block(
                state,
                "graphql",
                "unscoped measurement-page error",
                errors=scoped["unscoped"],
            )
            return "aborted"
        branch_errors = scoped["branches"].get(experiment_id, [])
        data = response.get("data")
        experiment = data.get("experiment") if isinstance(data, dict) else None
        measurements = experiment.get("measurements") if isinstance(experiment, dict) else None
        if branch_errors:
            # path 指到本实验（含其 measurements 分支）：保留到分支，其他分支继续。
            if measurements is not None:
                self._block(
                    state,
                    "protocol",
                    "scoped error must null the failed branch",
                    experiment=experiment_id,
                )
                return "aborted"
            branch["error"] = {
                "code": (branch_errors[0].get("extensions") or {}).get("code", "BRANCH_ERROR"),
                "message": branch_errors[0].get("message", "branch unreadable"),
                "path": branch_errors[0].get("path"),
                "page_index": branch["next_page_index"],
            }
            branch["attempts"] += 1
            branch["retries_this_run"] = branch.get("retries_this_run", 0) + 1
            return "ok"
        if not isinstance(experiment, dict) or experiment.get("id") != experiment_id:
            self._block(
                state,
                "protocol",
                "measurement response experiment mismatch",
                experiment=experiment_id,
            )
            return "aborted"
        if not isinstance(measurements, dict) or not isinstance(measurements.get("pageInfo"), dict):
            # data 没报错但分支结构缺失：未知范围，不能当空数组。
            self._block(
                state,
                "protocol",
                "measurements branch missing without scoped error",
                experiment=experiment_id,
            )
            return "aborted"
        edges = measurements.get("edges")
        page_info = measurements["pageInfo"]
        if not isinstance(edges, list) or not isinstance(page_info.get("hasNextPage"), bool):
            self._block(
                state, "protocol", "measurements page malformed", experiment=experiment_id
            )
            return "aborted"
        end_cursor = page_info.get("endCursor")
        if end_cursor is not None and not isinstance(end_cursor, str):
            self._block(state, "protocol", "inner endCursor must be text or null")
            return "aborted"
        for edge in edges:
            if not isinstance(edge, dict) or not isinstance(edge.get("node"), dict):
                self._block(
                    state,
                    "protocol",
                    "measurement edge malformed",
                    experiment=experiment_id,
                )
                return "aborted"
            node = edge["node"]
            row_id = node.get("id")
            if not isinstance(row_id, str) or not row_id:
                self._block(
                    state,
                    "protocol",
                    "measurement id missing",
                    experiment=experiment_id,
                )
                return "aborted"
            fingerprint = digest(node)
            # 同一 ID 重现时核对内容，不一致必须中止而不是静默覆盖。
            if row_id in state["rows"] and state["rows"][row_id]["digest"] != fingerprint:
                self._block(
                    state,
                    "content_conflict",
                    "measurement id reappeared with different content",
                    experiment=experiment_id,
                    measurement=row_id,
                )
                return "aborted"
            if row_id not in state["rows"]:
                record = {
                    "id": row_id,
                    "experiment": experiment_id,
                    "digest": fingerprint,
                    "node": node,
                    "page_index": branch["next_page_index"],
                }
                state["rows"][row_id] = record
                state["row_order"].append(row_id)
            branch["measurement_ids"].append(row_id)
        has_next = page_info["hasNextPage"]
        if has_next:
            if end_cursor is None:
                self._block(
                    state,
                    "pagination",
                    "inner hasNextPage without endCursor",
                    experiment=experiment_id,
                )
                return "aborted"
            if end_cursor in branch["seen_cursors"] or end_cursor == branch["after"]:
                self._block(
                    state,
                    "pagination",
                    "inner cursor stalled or cycling",
                    experiment=experiment_id,
                    cursor=end_cursor,
                )
                return "aborted"
            branch["seen_cursors"].append(end_cursor)
            branch["after"] = end_cursor
        else:
            branch["complete"] = True
        branch["end_cursor"] = end_cursor
        branch["has_next_page"] = has_next
        branch["next_page_index"] += 1
        branch["error"] = None
        return "ok"
    # ------------------------------------------------------------------ 发布
    def publish(self, harvest_id, request_id, classification, scopes, expected_version=None):
        state = self.store.load(harvest_id)
        name(request_id)
        if state["status"] == "published":
            publication = state["publication"]
            if publication["request_id"] != request_id:
                reject("conflict", "harvest already published under another request id")
            return {**publication, "reused": True}
        if state["status"] != "complete":
            reject("state", "only complete harvests can publish", status=state["status"])
        if expected_version is None:
            expected_version = self._current_head(state["dataset"])
        integer(expected_version, 0)
        records = self._lineage_records(state)
        lineage = self.store.write_lineage(harvest_id, records)
        state["lineage_artifact"] = lineage
        dataset_id = state["dataset"]
        rows = [state["rows"][row_id]["node"] for row_id in state["row_order"]]
        content = (
            "\n".join(canonical(row) for row in rows) + ("\n" if rows else "")
        ).encode()
        metadata = {
            "kind": "graphql_frozen_batch_harvest",
            "harvest_id": harvest_id,
            "batch_id": state["batch_id"],
            "revision": state["revision"],
            "lineage_artifact": lineage["sha256"],
            "lineage_records": lineage["records"],
            "source_pages": len(state["pages"]),
            "source_requests": state["requests_made"],
        }
        # 最终入库完全复用既有通道：schema 校验、artifact 摘要、expected_version 冲突检测。
        result = self.ingestion.ingest(
            request_id,
            dataset_id,
            state["schema_version"],
            content,
            "jsonl",
            classification,
            scopes,
            parents=[],
            metadata=metadata,
            expected_version=expected_version,
        )
        if not result.get("reused"):
            state["status"] = "published"
            state["publication"] = {**result, "request_id": request_id}
            self.store.save(state)
            with self.db.transaction():
                self.db.audit(
                    "harvest_published",
                    harvest_id,
                    {"request": request_id, "result": result, "revision": state["revision"]},
                )
        return {**result, "request_id": request_id, "reused": result.get("reused", False)}

    # ------------------------------------------------------------------ 查询
    def status(self, harvest_id):
        return self._summary(self.store.load(harvest_id))

    def lineage(self, harvest_id, measurement_id=None):
        state = self.store.load(harvest_id)
        records = self._lineage_records(state)
        if measurement_id is not None:
            records = [record for record in records if record["measurement_id"] == measurement_id]
            if not records:
                reject("missing", "measurement not found in harvest", measurement=measurement_id)
        return {
            "harvest_id": harvest_id,
            "revision": state["revision"],
            "records": records,
        }

    def list_harvests(self):
        return [self.status(harvest_id) for harvest_id in self.store.list_harvests()]

    # ------------------------------------------------------------------ 辅助
    @staticmethod
    def _block(state, code, message, **details):
        state["blockers"] = [{"code": code, "message": message, "details": details}]

    @staticmethod
    def _effective_budget(state):
        """可用额度 = 本地充值与服务端剩余额度的严格下限。

        从未充值（local_credit 为 None）时不设本地闸门，仅由服务端额度/429 约束；
        本地一旦充值，本地余额成为硬上限。服务端额度在 429 后或本地充值后视为
        过期（stale），下一次成功响应前不参与计算，避免旧的 0 值挡住恢复请求。
        """
        local = state.get("local_credit")
        server = None
        if not state.get("server_budget_stale") and not state.get("server_rejected_budget"):
            server = state.get("server_remaining_cost")
        candidates = [value for value in (local, server) if value is not None]
        return min(candidates) if candidates else None

    def _request(self, state, client, operation, variables):
        """发一次请求，落地原始响应与预算；任何失败都保留可恢复状态。"""
        effective = self._effective_budget(state)
        if effective is not None and effective <= 0:
            state["budget_exhausted"] = True
            self._block(
                state,
                "budget_exhausted",
                "cost budget exhausted; resume with budget_credit",
                effective_remaining=effective,
            )
            return None
        try:
            response = client.post(operation, variables)
        except HarvestTransportError as exc:
            if exc.code == "budget_exhausted":
                # 服务端 429：它的额度信号已不可信，在下次成功响应前不参与预算计算。
                state["server_rejected_budget"] = True
                state["server_budget_stale"] = True
                state["budget_exhausted"] = (
                    state.get("local_credit") is None
                    or state["local_credit"] <= 0
                )
            state["blockers"] = [exc.as_dict()]
            self.store.save(state)
            return None
        state["requests_made"] += 1
        # 本地充值按实际消耗递减，与服务端计费独立核对；未充值时保持 None。
        if state.get("local_credit") is not None:
            state["local_credit"] = max(0, state["local_credit"] - 1)
        extensions = response["extensions"] or {}
        if "remainingCost" in extensions:
            remaining = extensions["remainingCost"]
            if not isinstance(remaining, int) or isinstance(remaining, bool) or remaining < 0:
                self._block(
                    state,
                    "protocol",
                    "remainingCost must be a nonnegative integer",
                )
                self.store.save(state)
                return None
            state["server_remaining_cost"] = remaining
            # 429 后或本地充值后的首次成功响应刷新了服务端额度，重新信任其 extensions。
            state["server_rejected_budget"] = False
            state["server_budget_stale"] = False
            # 预算变化立刻落盘，保证本轮中止后重启看到的是服务端最新额度与本地余额。
            self.store.save(state)
        if self._effective_budget(state) == 0:
            state["budget_exhausted"] = True
        # 冻结 revision 与外层/内层游标严格分离：revision 只来自批次与 extensions，不接受游标位置混用。
        if "revision" in extensions and state["revision"] is not None:
            if extensions["revision"] != state["revision"]:
                state["frozen_expired"] = {
                    "code": "frozen_revision_expired",
                    "message": "extension revision no longer matches frozen revision",
                    "details": {
                        "expected": state["revision"],
                        "observed": extensions["revision"],
                    },
                }
                state["blockers"] = [state["frozen_expired"]]
                self.store.save(state)
                return None
        record = {
            "sequence": len(state["pages"]),
            "operation": operation,
            "request": response["request"],
            "http_status": response["http_status"],
            "data": response["data"],
            "errors": response["errors"],
            "extensions": response["extensions"],
        }
        checksum = self.store.write_page(state["harvest_id"], record)
        state["pages"].append(
            {
                "sequence": record["sequence"],
                "operation": operation,
                "variables": variables,
                "artifact": checksum,
                "error_paths": [error.get("path") for error in response["errors"]],
            }
        )
        return response

    def _classify_errors(self, response, current_experiment=None):
        result = {"unscoped": [], "branches": {}, "revision_expired": False}
        for error in response["errors"]:
            if not isinstance(error, dict):
                result["unscoped"].append(error)
                continue
            code = (error.get("extensions") or {}).get("code")
            path = error.get("path")
            if code == "EXPIRED" or (isinstance(path, list) and path and path[0] == "batch"):
                result["revision_expired"] = True
                result["unscoped"].append(error)
                continue
            target = self._path_branch(path, current_experiment)
            if target is None:
                result["unscoped"].append(error)
            else:
                result["branches"].setdefault(target, []).append(error)
        return result

    @staticmethod
    def _path_branch(path, current_experiment):
        # Measurements 形态：["experiment", ...]，实验 ID 在请求变量里，归到当前拉取的分支。
        if (
            current_experiment is not None
            and isinstance(path, list)
            and path
            and path[0] == "experiment"
        ):
            return current_experiment
        return None

    def _new_branch(self, experiment_id, discovery_cursor):
        return {
            "experiment_id": experiment_id,
            "after": None,
            "end_cursor": None,
            "has_next_page": None,
            "complete": False,
            "seen_cursors": [],
            "next_page_index": 0,
            "measurement_ids": [],
            "error": None,
            "attempts": 0,
            "retries_this_run": 0,
            "discovery_cursor": discovery_cursor,
        }

    @staticmethod
    def _next_open_branch(state):
        """两阶段调度：先推进所有无错分支（让成功的实验先取完），
        健康分支全部处理完后，仅在允许时于限次内重试带错分支（只补失败/缺失页）。"""
        for experiment_id, branch in state["experiments"].items():
            if not branch["complete"] and branch["error"] is None:
                return experiment_id
        if not state.get("retry_failed_branches", True):
            return None
        for experiment_id, branch in state["experiments"].items():
            if (
                not branch["complete"]
                and branch["error"] is not None
                and branch.get("retries_this_run", 0) < state.get("max_branch_retries", 3)
            ):
                state["retry_phase"] = True
                return experiment_id
        return None

    @staticmethod
    def _has_open_branch(state):
        return any(not branch["complete"] for branch in state["experiments"].values())

    def _lineage_records(self, state):
        # 数据行 -> 实验 -> 原页 artifact -> 请求参数，全部可从行向前追溯。
        records = []
        for row_id in state["row_order"]:
            row = state["rows"][row_id]
            page_ref = None
            for page in state["pages"]:
                if page["operation"] != "Measurements":
                    continue
                if page["variables"]["experiment"] != row["experiment"]:
                    continue
                envelope = self.store.read_page(state["harvest_id"], page["artifact"])
                edges = (
                    ((envelope["data"] or {}).get("experiment") or {}).get("measurements") or {}
                ).get("edges", [])
                if any(edge.get("node", {}).get("id") == row_id for edge in edges):
                    page_ref = {
                        "sequence": page["sequence"],
                        "artifact": page["artifact"],
                        "variables": page["variables"],
                        "operation": page["operation"],
                    }
                    break
            records.append(
                {
                    "measurement_id": row_id,
                    "experiment_id": row["experiment"],
                    "node_digest": row["digest"],
                    "source_page": page_ref,
                    "batch_id": state["batch_id"],
                    "revision": state["revision"],
                }
            )
        return records

    def _current_head(self, dataset_id):
        head = self.db.one(
            "SELECT MAX(version) AS n FROM versions WHERE dataset=?",
            (dataset_id,),
        )
        return (head["n"] if head else None) or 0

    def _summary(self, state, reused=None):
        expected_head = self._current_head(state["dataset"])
        summary = {
            "harvest_id": state["harvest_id"],
            "status": state["status"],
            "batch_id": state["batch_id"],
            "revision": state["revision"],
            "dataset": state["dataset"],
            "schema_version": state["schema_version"],
            "outer_complete": state["outer"]["complete"],
            "outer_after": state["outer"]["after"],
            "experiments": [
                {
                    "id": experiment_id,
                    "complete": branch["complete"],
                    "after": branch["after"],
                    "measurements": len(branch["measurement_ids"]),
                    "error": branch["error"],
                    "attempts": branch["attempts"],
                }
                for experiment_id, branch in sorted(state["experiments"].items())
            ],
            "rows": len(state["row_order"]),
            "pages": len(state["pages"]),
            "requests_made": state["requests_made"],
            "remaining_cost": self._effective_budget(state),
            "server_remaining_cost": state.get("server_remaining_cost"),
            "local_credit": state.get("local_credit"),
            "budget_exhausted": state["budget_exhausted"],
            "blockers": state["blockers"],
            "expected_head": expected_head,
            "publication": state.get("publication"),
        }
        if reused is not None:
            summary["reused"] = reused
        return summary
