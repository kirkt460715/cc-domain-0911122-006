import urllib.error
import urllib.request

from ..validation import canonical, parse_json, reject

# 演练服务只分派固定操作；query 文本保留在请求记录里以便审计，不参与解析。
QUERIES = {
    "OpenBatch": (
        "query OpenBatch { batch { id revision } }"
    ),
    "Experiments": (
        "query Experiments($revision: ID!, $after: String) { "
        "experiments(revision: $revision, after: $after) { "
        "edges { cursor node { id } } "
        "pageInfo { endCursor hasNextPage } } }"
    ),
    "Measurements": (
        "query Measurements($revision: ID!, $experiment: ID!, $after: String) { "
        "experiment(id: $experiment, revision: $revision) { id "
        "measurements(after: $after) { "
        "edges { cursor node { id value group } } "
        "pageInfo { endCursor hasNextPage } } } }"
    ),
}


class HarvestTransportError(Exception):
    """传输层或非 GraphQL 协议错误，调用方应当保留可恢复状态。"""

    def __init__(self, code, message, **details):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def as_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}


class BudgetExhausted(HarvestTransportError):
    def __init__(self, message, **details):
        super().__init__("budget_exhausted", message, **details)


class GraphQLClient:
    """最小 GraphQL over HTTP 客户端：HTTP 200 也可能同时携带 data 与 errors。"""

    def __init__(self, base_url, timeout=5):
        if not isinstance(base_url, str) or not base_url.startswith(("http://", "https://")):
            reject("input", "graphql endpoint must be an http(s) URL")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def post(self, operation, variables):
        if operation not in QUERIES:
            reject("input", "unknown graphql operation", operation=operation)
        body = {
            "operationName": operation,
            "query": QUERIES[operation],
            "variables": variables,
        }
        raw = canonical(body).encode()
        request = urllib.request.Request(
            self.base_url + "/graphql",
            data=raw,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status = response.status
                payload = response.read()
        except urllib.error.HTTPError as exc:
            # 429 是预算信号，其余 HTTP 错误视为可重试的传输失败。
            payload = exc.read()
            if exc.code == 429:
                raise BudgetExhausted(
                    "platform rejected request: cost budget exhausted",
                    http_status=exc.code,
                    body=self._safe_body(payload),
                )
            raise HarvestTransportError(
                "http_error",
                "graphql request failed",
                http_status=exc.code,
                body=self._safe_body(payload),
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise HarvestTransportError("transport", "graphql endpoint unreachable", reason=str(exc))
        try:
            envelope = parse_json(payload)
        except Exception:
            raise HarvestTransportError(
                "protocol", "response was not valid JSON", http_status=status
            )
        if not isinstance(envelope, dict):
            raise HarvestTransportError("protocol", "response envelope must be an object")
        if "data" not in envelope and "errors" not in envelope:
            raise HarvestTransportError("protocol", "response lacks data and errors")
        errors = envelope.get("errors")
        if errors is not None and not isinstance(errors, list):
            raise HarvestTransportError("protocol", "errors must be a list")
        extensions = envelope.get("extensions")
        if extensions is not None and not isinstance(extensions, dict):
            raise HarvestTransportError("protocol", "extensions must be an object")
        return {
            "http_status": status,
            "data": envelope.get("data"),
            "errors": errors or [],
            "extensions": extensions or {},
            "request": body,
        }

    @staticmethod
    def _safe_body(payload):
        try:
            return parse_json(payload)
        except Exception:
            return payload[:256].decode("utf-8", "replace")
