## Why
Loadable media calls discard host session identity. Native control handlers cannot reply through a dedicated channel, so management RPC must share media outputs and scheduling.

## What Changes
- Forward an owned session/node context through a new processing entry point.
- Add typed RPC requests, method descriptors and reply helpers, with an asynchronous ABI reply sink.
- Route opted-in RPC independently of media in the companion host worktree.
- Preserve the existing control-bus wire protocol and existing source entry points.
- **BREAKING**: bump ABI to 0.5 and plugin-sdk to 0.7; test binary version rejection and document rebuild requirements.

## Impact
- SDK: traits, loadable-node-abi, plugin-sdk adapter, tests and documentation.
- Host: loadable adapter and session router in the companion remotemedia-sdk-base worktree.
- Clients: propagate RPC subscription failures through Python and JavaScript proxies, and permit Python stream cancellation addresses in the companion remotemedia-sdk worktree.
- Authorized by the user's active implementation goal following the design discussion.
