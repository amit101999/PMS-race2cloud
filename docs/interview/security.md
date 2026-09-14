# Interview Topic: Security in PMS

Labels: mostly **[FOUND]** gaps plus **[PROPOSED]** fixes. Evidence: [open-findings](../problems-and-solutions/open-findings.md).

The honest position: **the current API has no authentication.** In an interview, say so plainly and explain how you'd fix it. Don't claim security controls that don't exist.

---

## What we actually have [BUILT]

- **CORS** restricted to `localhost:3000` and `localhost:5000`. This only limits browsers, not other callers.
- **Quote-escaping helpers** (`esc` / `escSql`) in many ZCQL queries, but not all of them.
- **Input validation in custody matching:** date regex, ISIN pattern, exact header checks, 25 MB upload cap with `abortOnLimit`.
- **Server-side gates** in custody matching instead of UI-only disabled buttons.
- **Presigned download URLs** (1 hour) for custody files, rather than proxying bucket access.
- **Fail-safe cleanup:** dry run by default, a retention floor, and it skips active runs.

## What's missing [FOUND]

- No authentication or authorization on any route.
- Some ZCQL queries interpolate request values without escaping (`CashController.js:34`, `DividendUploader.js:297, 762, 777, 868, 884`).
- The old transaction upload route bypasses the custody gate.
- The bulk-callback endpoint doesn't verify its caller.
- No upload size limits on the transaction and bhav routers.

## Interview questions

### Basic

**Q: How does authentication work in your project?**
A: Honestly, it doesn't yet. The Express API on AppSail has no auth middleware; it was built as an internal tool. The first thing I'd add is platform authentication or a token check in front of every `/api` route, then role checks on anything that writes data.

**Q: How do you prevent SQL injection?**
A: Our database is Catalyst Data Store queried with ZCQL strings. Most queries escape single quotes through a helper, but I found a few that put request values straight into the query, for example the cash balance summary's account code. The fix is to escape every value in one place and validate formats like ISIN at the boundary. In a Java rebuild I'd use bound parameters so it can't happen at all.

### Intermediate

**Q: What's the difference between CORS and authentication?**
A: CORS tells a browser which websites may call the API. It does nothing against curl or a script. Our CORS allowlist is localhost only, but without authentication anyone reaching the URL could still call it.

**Q: You enforce the custody gate on the server. Is that enough?**
A: The new endpoints do check on the server, which matters more than disabling a button. But the old upload endpoint is still mounted, so the gate can be skipped. Security is only as strong as the weakest route.

**Q: What's IDOR, and does it apply here?**
A: IDOR means changing an ID in a request to reach someone else's data. Here any account code works on analytics and export endpoints, because nothing checks who is asking. It applies fully until we add authentication and ownership checks.

### Scenario / follow-up

**Q: A webhook tells your system a bulk job finished. How do you trust it?**
A: Right now we don't verify it. I'd generate a random secret when submitting the bulk job, include it in the callback URL, and reject callbacks that don't carry it.

**Q: What would you log, and what wouldn't you?**
A: Log job names, run ids, statuses and counts. Don't log tokens or full client data. Our bulk-callback handler logs the whole raw body, which I'd trim.

**Q: What would you fix first, and why?**
A:
1. Authentication, because every other issue is worse without it.
2. The unescaped queries.
3. Removing the bypass route.
4. A callback secret.

## Quick revision

- No auth today; say it honestly and give the fix.
- CORS is not security against non-browser callers.
- Some ZCQL values are unescaped; fix with one helper and format validation (bound parameters in Java).
- The old upload route bypasses the custody gate.
- Server-side gates plus presigned URLs are the good parts.
