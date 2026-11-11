# Negative Holdings

A separate, read-only app that lists clients who sold more of a share than they held, with the exact minus value.

It has two parts:

- **`functions/NegativeHoldingsApi/`:** a Catalyst Advanced I/O function that reads the database. Running inside Catalyst, it gets its access automatically, so it needs no tokens or secrets.
- **`client/`:** a React page, built to static files that any host can serve.

Nothing in the main app imports from this folder. This folder has its own `catalyst.json`, so the root one is not involved.

## Why these clients don't show up in the main app

Every holdings job caps a sell at what is held, so the stored holding never goes below zero. If a client holds 100 and sells 150, the main app shows 0, not −50.

The sell row keeps its full quantity, so this app compares it with the holding on the row before and shows the exact value. **Short by** = sell quantity − previous holding. **Exact holding** = stored holding − total short so far.

This app does not change the main app, which still shows 0. If a split, merger or demerger follows a shortfall, the old shortfall can't be converted reliably, so the page shows the last exact value before that event and marks the share **Check by hand**.

| Row | Quantity | Stored holding (main app) | Short so far | Exact holding |
|---|---|---|---|---|
| Buy | 100 | 100 | 0 | 100 |
| Sell | 150 | 0 | 50 | −50 |
| Buy | 80 | 80 | 50 | 30 |

The rules are in `functions/NegativeHoldingsApi/negativeHoldings.js`.

## Routes

All are GET and read-only. The base is the function URL ending in `/execute`.

| Route | Returns |
|---|---|
| `/accounts` | The account list |
| `/negative-holdings?accountCode=&asOnDate=` | One account's shortfalls |
| `/share-history?accountCode=&isin=&asOnDate=` | One share's history, with short by and exact holding |

## Deploy the function

You need the Catalyst CLI, logged in with `catalyst login`. Run these from this folder, not from the repo root:

```
npm run install:function
catalyst deploy --only functions:NegativeHoldingsApi
```

This folder's `.catalystrc` is a copy of the root one. Before deploying, check that the active environment is the one you want.

The URL printed by the deploy is missing `/execute`. Add it to get the base URL, then check it in a browser. It should list the accounts:

```
https://YOUR-PROJECT-DOMAIN/server/NegativeHoldingsApi/execute/accounts
```

## Build and host the page

1. Copy `client/.env.example` to `client/.env` and set `REACT_APP_API_URL` to the base URL. On a hosting platform, set it in the build settings instead.
2. Run `npm run build`.
3. Host the `client/build` folder on any static host.
4. In the Catalyst console, go to Authentication → Authorized Domains and add the page's domain. Without this, the browser blocks the calls. This setting also applies to the project's AppSail backend.

The function must not set its own CORS header for that domain, or the browser rejects the response. It sets one only for `localhost`, for testing.

### Try the page from your computer

With `client/.env` set and the function deployed:

```
cd client
npm install
npm start
```

## Test

```
npm test
```

## Limits

- **30 seconds per request.** The page checks one account per request. An account that takes too long is listed as failed, with **Retry**, and never looks clean.
- **Public.** Catalyst functions are public by default and this page has no login, so anyone with the URL can read every client's holding history and start scans. Adding a login is recommended before real client data is used.
- **Admin access.** The function has full access to the project's data. Only its code keeps it to SELECT queries.
- **Notes.** Review status and notes are saved only in the browser that entered them.

## Undo

Delete the function in the Catalyst console, remove the domain from Authorized Domains, and take down the page.

## Copied code

These are copies, so change them when the originals change:

| Here | Original |
|---|---|
| `functions/NegativeHoldingsApi/holdingsRead.js` | `appsail-nodejs/util/analytics/holdingsFromTable.js` |
| `functions/NegativeHoldingsApi/allAccountCodes.js` | `appsail-nodejs/util/allAccountCodes.js` |
| `getShareHistory` in `functions/NegativeHoldingsApi/index.js` | `getStockTransactionHistory` in `appsail-nodejs/controller/TransactionController.js` |
| `client/src/TransactionPage.js`, `TransactionPage.css` | `react-app/src/pages/TransactionPage/` |
| `client/src/Pagination.js` | `react-app/src/components/common/CommonComponents.js` |
| `client/src/design-system.css` | `react-app/src/styles/design-system.css` |
