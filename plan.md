# Iteration 4 implementation plan

唯一实施范围和验收标准见 `file:///tmp/goal_iter_20260713_174459/frozen/FROZEN_PROBLEM_LIST.md`。

- [x] I4-01 修复全部冻结 errcheck finding。
- [x] I4-02 禁止 Redis failover coordinator 用于 strict OwnerLease。
- [x] I4-03 coordinator error 下 OwnerLease delivery fail-closed。
- [x] I4-04 generation floor advance failure 下回滚注册和 SSE session。
- [x] I4-05 建立 PR/main/tag API compatibility baseline gate。
- [x] I4-06 统一 Go 1.26.5 commands。
- [ ] I4-07 等待宿主部署身份。禁止虚构生产证据。
