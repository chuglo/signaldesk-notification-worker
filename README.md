# signaldesk-notification-worker

**Not for production use.**

Lease-fenced worker that consumes `notification.requested.v1`, asks the authoritative control API to reconcile one email delivery, attaches its identifier to the notification, and ACKs the stream entry. It never handles recipient addresses, SMTP, Mailpit, credentials in payloads, or diagnostic result payloads.

Run once with `signaldesk-notification-worker --once`; use `--ready` for dependency readiness.

## License

MIT. See [LICENSE](LICENSE).
