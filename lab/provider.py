import threading
from .http import ProtocolError, main


class Provider:
    """演练用 GraphQL 服务。

    fail_attempts 控制 expA 第二页连续失败次数（默认 1，与历史行为一致），
    让预算中断、进程重启等场景可以精确复现“下一次请求仍失败/已恢复”。
    """

    def __init__(self, cost=50, fail_once=None, fail_attempts=1):
        self.lock = threading.RLock()
        self.revision = 'r1'
        if fail_once is not None:
            fail_attempts = 1 if fail_once else 0
        self.fail_attempts = fail_attempts
        self.cost = cost
        self.calls = []

    def handle(self, method, path, args, body):
        with self.lock:
            if path == '/control':
                if body.get('expire'):
                    self.revision = 'r2'
                if body.get('credit') is not None:
                    self.cost += int(body['credit'])
                return {'revision': self.revision, 'cost': self.cost}
            if path != '/graphql' or method != 'POST':
                raise ProtocolError(404, 'route')
            op = body['operationName']
            v = body.get('variables', {})
            self.calls.append(body)
            self.cost -= 1
            if self.cost < 0:
                raise ProtocolError(429, 'budget')
            ext = {'remainingCost': self.cost, 'revision': self.revision}
            if op == 'OpenBatch':
                return {'data': {'batch': {'id': 'batch1', 'revision': self.revision}}, 'extensions': ext}
            if v.get('revision') != self.revision:
                return {
                    'data': None,
                    'errors': [
                        {
                            'message': '冻结批次已过期',
                            'path': ['batch'],
                            'extensions': {'code': 'EXPIRED'},
                        }
                    ],
                    'extensions': ext,
                }
            if op == 'Experiments':
                page = int(v.get('after') or 0)
                name = ['expA', 'expB'][page]
                return {
                    'data': {
                        'experiments': {
                            'edges': [{'cursor': str(page + 1), 'node': {'id': name}}],
                            'pageInfo': {'endCursor': str(page + 1), 'hasNextPage': page == 0},
                        }
                    },
                    'extensions': ext,
                }
            if op == 'Measurements':
                exp = v['experiment']
                page = int(v.get('after') or 0)
                if exp == 'expA' and page == 1 and self.fail_attempts > 0:
                    self.fail_attempts -= 1
                    return {
                        'data': {'experiment': {'id': exp, 'measurements': None}},
                        'errors': [
                            {
                                'message': '测量分支暂不可读',
                                'path': ['experiment', 'measurements'],
                                'extensions': {'code': 'RETRY'},
                            }
                        ],
                        'extensions': ext,
                    }
                row = {
                    'id': f'{exp}-{page + 1}',
                    'value': str((page + 1) * 10),
                    'group': 'A' if exp == 'expA' else 'B',
                }
                return {
                    'data': {
                        'experiment': {
                            'id': exp,
                            'measurements': {
                                'edges': [{'cursor': str(page + 1), 'node': row}],
                                'pageInfo': {
                                    'endCursor': str(page + 1),
                                    'hasNextPage': page == 0,
                                },
                            },
                        }
                    },
                    'extensions': ext,
                }
            raise ProtocolError(400, 'operation')


if __name__ == '__main__':
    main(Provider())
