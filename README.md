# Threads provider

Install from the Cupboard and connect a separate Threads developer app in Settings → Integrations. Configure the connected `userId`. The host holds the OAuth grant and injects it into `host.fetch`; the package receives no token. Replies require a root owned by the connected account. Keyword search, mention management and deletion are not declared capabilities.

Each pass watches at most 25 recent root posts, then pages each flattened `/conversation` edge. Original parent ids, root owner, empty text and timestamps survive normalization. In-flight cursors retain the original watermark and emitted identities. Completed passes rescan with timestamp plus boundary ids. A conversation exceeding `maxPagesPerMedia` (maximum 50) fails explicitly. Conversations outside the watched root window are not covered.

Text and one optional image or video use container creation followed by publication. Text is limited to 500 graphemes. The package requires a `FINISHED` container status before publishing; it observes processing at one-minute intervals for at most five minutes. Confirmed readiness replaces an estimated processing delay. Both writes are issued once. Ambiguous publication preserves its container id and one status observation for reconciliation. The host retains destination, disclosure, quota and publish gates.

Verification is against recorded HTTP fakes and the SDK kit. **Live verification pending.** Tests make no vendor calls.

Primary references checked 2026-10-16: [publishing](https://developers.facebook.com/docs/threads/posts/), [container lifecycle](https://developers.facebook.com/docs/threads/troubleshooting/), [conversation reads](https://developers.facebook.com/docs/threads/retrieve-and-manage-replies/replies-and-conversations/). The package reuses the SDK contract and generated daemon launcher.
