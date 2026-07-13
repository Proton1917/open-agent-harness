# Contributing to open-agent-harness

Thank you for helping build a coding-agent harness that belongs to its users. Contributions are welcome from anyone, anywhere, provided they preserve the project’s technical independence and open-source boundary.

## Non-negotiable requirements

### 1. The harness core stays Rust; repository tooling stays practical

The primary implementation, executable, agent loop, model protocol, permission boundary, persistence, and built-in tools must remain Rust. This is a Rust project, not a language-purity contest.

- Transparent Shell or other source-based helpers are allowed when they are the practical choice for auditing, building, testing, packaging, releasing, fixture generation, and repository maintenance. `scripts/audit-harness.sh` is an intentional part of the quality gate and must not be removed merely to make the language count look purer.
- Keep ancillary tooling narrow, readable, and replaceable. It must not conceal an alternative harness core, require a proprietary runtime, download opaque executable logic, or turn the Rust binary into a thin launcher for another implementation.
- Native libraries and FFI require an explicit security, licensing, portability, and maintenance justification. Opaque binaries and closed-source runtime dependencies are not accepted.
- Prefer the Rust standard library and existing dependencies. A new crate must be open source, have a declared SPDX-compatible license, and be justified in the pull request.

### 2. AI assistance does not lower the bar

AI-assisted contributions are allowed. The submitter nevertheless owns every line and must be able to review, explain, test, and maintain it.

- Do not submit bulk-generated code that you have not read or cannot explain.
- Disclose which parts were AI-assisted and how you verified them. Maintainers may request a line-by-line explanation; inability to provide one is grounds to close the pull request.
- Do not leave placeholders, dead branches, speculative abstractions, fabricated compatibility claims, or warning suppressions that merely hide a defect.
- Every behavioral claim needs a test, reproducible evidence, or a precise source-code argument.
- Keep patches coherent and reviewable. Generated volume is never a substitute for correctness.
- Maintainers may reject technically compiling code when its invariants, failure behavior, privacy boundary, or ownership cannot be audited.

### 3. Obtain comparison material yourself

Before submitting code, every contributor must independently obtain their own lawful, authorized local copy of a relevant Claude Code release or equivalent first-party comparison material. This repository does not publish or redistribute those materials. The requirement exists so contributors can check observable behavior themselves instead of repeating an AI-generated compatibility claim; it does not authorize access, copying, or redistribution.

- Follow the license, terms, and laws that apply to you.
- Do not commit or upload vendor binaries, extracted bundles, bytecode, native modules, decompiled source, proprietary system prompts, credentials, account data, telemetry captures containing personal data, or copyrighted documentation.
- Do not attach such material to issues, pull requests, CI artifacts, releases, gists, or external mirrors operated for this project.
- Keep personal comparison material outside Git, under the ignored `reference/` directory or another private location.
- Evidence in a pull request should describe observable behavior, inputs, outputs, invariants, and test cases. It must not reproduce proprietary implementation text.

The implementation submitted here must be an original Rust reimplementation of general harness behavior. Copying minified/decompiled functions, identifiers, prompts, comments, assets, or native code is not acceptable.

## Project boundary

In scope:

- provider-neutral model I/O;
- message normalization, streaming, retries, and context accounting;
- local tools, permissions, workspace boundaries, sessions, compaction, tasks, and agent orchestration;
- open protocols and user-configured integrations;
- privacy, resource limits, deterministic tests, and security hardening.

Out of scope:

- vendor accounts, subscriptions, billing, identity, entitlement, or region checks;
- telemetry, experimentation platforms, marketing features, or hidden remote services;
- proprietary UI cloning, brand prompts, copyrighted assets, or compatibility code that sends data to a hard-coded vendor endpoint;
- any feature that requires closed infrastructure to function.

## Development workflow

1. Read the local, ignored `AGENTS.md` if one is present.
2. Start from a clean `main` branch and keep unrelated changes out of the patch.
3. Add success, failure, boundary, and privacy tests with the implementation.
4. Use local mock servers for protocol tests. Tests must not contact public services.
5. Never place real credentials, emails, device identifiers, account identifiers, or private repository data in fixtures or logs.
6. Update `README.md` and `MIGRATION.md` when public behavior changes.
7. Run the complete verification gate:

The repository-level `.cargo/config.toml` promotes every compiler warning to an error. A pull request with even one warning is not mergeable, and the release build log must contain no warnings.

```bash
cargo fmt --all -- --check
cargo +1.85.0 check --locked --all-targets
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
scripts/audit-harness.sh
```

## Pull-request checklist

- [ ] The harness core and primary executable remain Rust; ancillary tooling is transparent, scoped, and justified.
- [ ] I independently obtained lawful, authorized comparison material; none of it is included in this contribution.
- [ ] I disclosed AI-assisted sections and can explain every submitted line without delegating responsibility to a model.
- [ ] The change is provider-neutral and has no hard-coded vendor endpoint.
- [ ] No proprietary comparison material is included or quoted.
- [ ] Tool inputs are schema-validated before permission or execution.
- [ ] Filesystem and network boundaries fail closed.
- [ ] Outputs, subprocesses, and responses have explicit resource limits.
- [ ] New persistence uses private permissions and does not store secrets unnecessarily.
- [ ] Tests cover success and failure paths without external network access.
- [ ] Formatting, tests, clippy, release build, and repository audit pass.
- [ ] The complete test and release compilation logs contain zero warnings.
- [ ] Documentation accurately describes what the code actually guarantees.

By submitting a contribution, you agree that your original contribution is licensed under this repository’s MIT License.

---

# 为 open-agent-harness 贡献代码

感谢你帮助建设一个真正属于使用者的 coding-agent harness。项目欢迎来自任何地区、任何人的贡献，前提是贡献必须守住技术独立与开源边界。

## 不可妥协的要求

### 1. Harness 核心坚持 Rust，仓库工具务求实用

主体实现、可执行程序、agent 循环、模型协议、权限边界、持久化和内置工具必须继续以 Rust 实现。这是一个 Rust 项目，但不是语言洁癖比赛。

- 审计、构建、测试、打包、发行、fixture 生成和仓库维护中，哪种工具方便、透明、可靠，就可以采用哪种源码工具；Shell 或其他辅助语言均可。`scripts/audit-harness.sh` 是质量门槛的一部分，不能为了让语言统计看起来更“纯”而删除。
- 辅助工具必须范围清楚、便于阅读、可以替换；不得借此藏入另一套 harness 核心、依赖专有运行时、下载黑盒可执行逻辑，或把 Rust 二进制变成其他实现的薄启动器。
- 原生库与 FFI 必须明确说明安全、许可证、可移植性和维护理由；不接受黑盒二进制与闭源运行时依赖。
- 优先使用 Rust 标准库和现有依赖。新增 crate 必须开源、声明兼容的 SPDX 许可证，并在 PR 中说明必要性。

### 2. 使用 AI 不会降低贡献门槛

允许使用 AI 辅助贡献，但提交者仍然必须对每一行代码负责，并能够审阅、解释、测试和维护它。

- 不得提交自己没有读过、无法解释的大批量生成代码。
- 必须披露哪些部分使用了 AI 辅助，以及自己如何完成验证。维护者可以要求逐行解释；无法解释即可关闭 PR。
- 不得留下占位实现、死分支、臆想抽象、伪造的兼容性声明，或仅用于掩盖缺陷的 warning 抑制。
- 每项行为声明都必须有测试、可复现证据或严密的源码论证。
- 补丁必须连贯、可审计；生成代码的数量永远不能替代正确性。
- 即使代码能够编译，只要其不变量、失败行为、隐私边界或来源责任无法审计，维护者仍可拒绝合并。

### 3. 比对材料必须自行取得

每一位贡献者在提交代码前，都必须自行、合法、经授权取得一份相关的 Claude Code 版本或等价的一方比对材料。本仓库不发布、也不转发这些材料。这项要求的目的，是让贡献者亲自核对可观察行为，而不是复读 AI 生成的兼容性声明；它不授权任何访问、复制或再分发行为。

- 遵守适用于你的许可证、服务条款与法律。
- 不得提交或上传厂商二进制、拆包产物、bytecode、原生模块、反编译源码、专有系统提示词、凭据、账户数据、含个人信息的遥测记录或受版权保护的文档。
- 不得把这些材料附在 issue、PR、CI artifact、release、gist，或本项目运营的任何外部镜像中。
- 个人比对材料应放在 Git 之外，例如被忽略的 `reference/` 目录或其他私有位置。
- PR 中的证据应描述可观察行为、输入、输出、不变量和测试用例，不得复现专有实现文本。

提交到本仓库的实现必须是对通用 harness 行为的原创 Rust 重写。不得复制压缩或反编译函数、标识符、提示词、注释、资产或原生代码。

## 项目边界

范围内：

- 提供方无关的模型 I/O；
- 消息规范化、流式响应、重试和上下文计量；
- 本地工具、权限、工作区边界、会话、压缩、任务和 agent 编排；
- 开放协议与用户自行配置的集成；
- 隐私、资源上限、确定性测试与安全加固。

范围外：

- 厂商账号、订阅、计费、身份、权益或地域检查；
- 遥测、实验平台、营销功能或隐藏远程服务；
- 专有 UI 复刻、品牌提示词、受版权保护资产，或会向硬编码厂商 endpoint 发送信息的兼容代码；
- 必须依赖闭源基础设施才能工作的功能。

## 开发流程

1. 如果本地存在被忽略的 `AGENTS.md`，先完整阅读。
2. 从干净的 `main` 开始，不要把无关改动混进补丁。
3. 实现必须同时提供成功、失败、边界和隐私测试。
4. 协议测试使用本地 mock server，测试不得访问公网服务。
5. fixture 和日志中绝不能出现真实凭据、邮箱、设备标识、账户标识或私有仓库数据。
6. 公开行为变化时同步更新 `README.md` 与 `MIGRATION.md`。
7. 执行完整验证门槛：

仓库级 `.cargo/config.toml` 会把所有编译器 warning 提升为 error。出现任何 warning 的 PR 均不得合并，release build 日志也必须保持零 warning。

```bash
cargo fmt --all -- --check
cargo +1.85.0 check --locked --all-targets
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
scripts/audit-harness.sh
```

## Pull Request 检查表

- [ ] Harness 核心与主体可执行程序仍以 Rust 实现；辅助工具透明、范围明确且理由充分。
- [ ] 已自行取得合法、经授权的比对材料；本贡献未包含其中任何内容。
- [ ] 已披露 AI 辅助部分，并能逐行解释提交内容，不把责任推给模型。
- [ ] 改动提供方无关，不含硬编码厂商 endpoint。
- [ ] 未包含或引用任何专有比对材料。
- [ ] 工具输入在权限判断和执行前完成 schema 校验。
- [ ] 文件系统与网络边界默认关闭、失败即拒绝。
- [ ] 输出、子进程和响应均有明确资源上限。
- [ ] 新增持久化使用私有权限，且不无谓保存秘密。
- [ ] 测试覆盖成功与失败路径，且不访问外部网络。
- [ ] 格式化、测试、clippy、release build 与仓库审计全部通过。
- [ ] 完整测试与 release 编译日志为零 warning。
- [ ] 文档只声明代码真正能够保证的行为。

提交贡献即表示你同意将自己的原创贡献按本仓库 MIT License 授权。
