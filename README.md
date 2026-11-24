# 有界缓冲下的多租户安全批次发布

原生 Windows + Python 3.12 标准库实现。输入为本地合成回执流，验证全部在本地完成。
完整设计与取舍见 [`docs/DESIGN.md`](docs/DESIGN.md)。

## 快速开始

```bash
python -m pytest                        # 全部测试（含十万条实测，约 20 秒）
python -m pytest tests/test_store.py    # 提交指针 / 恢复
python -m pytest tests/test_receiver.py # 两线程、去重、故障注入、租户隔离
python -m bench.run_benchmark 100000    # 生成 bench/reports/bench_100k.json
```

## 结构

| 文件 | 职责 |
|---|---|
| `secure_receiver/budget.py` | 8 MiB 硬上限下的五池独立字节账本（原子预占、峰值、池间转移） |
| `secure_receiver/durability.py` | Windows 目录 fsync（`FILE_FLAG_BACKUP_SEMANTICS` + 写访问）与二进制安全写入 |
| `secure_receiver/store.py` | 租户/批次目录、`segment.bin` + `COMMIT` 指针、恢复与未批准尾巴截断 |
| `secure_receiver/receiver.py` | 两生产者接收、去重/冲突、增量有界解压、末页摘要校验、发布/失败/重试 |
| `secure_receiver/streams.py` | 合成回执流（重叠窗口、冲突页、线程中止/崩溃、惰性流） |
| `tests/` | 账本、存储恢复、端到端流水线、基准 |
| `bench/` | 十万条离线基准与实测报告 |

## 关键性质

- **五池独立计数**：压缩输入、解压区、索引排队、待确认页、重试持有；之和受 8 MiB 硬上限约束，超限即失败。
- **末页摘要最后失败也不泄漏**：摘要页校验在前序页全部读入后进行；摘要错误 ⇒ 整批删除，索引/通知/读口均不可见。
- **目录已刷、指针未刷**：段已持久但无 `COMMIT` ⇒ 恢复判未批准并删除；指针之后被追加的字节视为未批准并截断。
- **线程中止 / 进程崩溃**：中止只失败当前批次；崩溃后新建接收端 `recover()` 后仅已批准批次可见，其他租户继续可读。
- **每页计费含对象开销**：`payload + 160 B`，使账本反映真实常驻内存而非下界（十万元实测见报告）。
