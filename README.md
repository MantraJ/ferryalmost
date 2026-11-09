# Ferry 1.6 (Vercel)

## Upload
1. In your GitHub repo, DELETE the old `api` folder if it exists, then upload everything in this folder, including the `public` folder (it holds index.html, the icons, manifest and sw.js; Vercel serves it directly, which is why the logo now shows on Vercel). Files must keep the same layout: `server.js`, `package.json`, `vercel.json` at the top, the rest inside `public/`.
2. Vercel redeploys by itself. Hard-refresh once (or remove and re-add the home-screen icon) to see the new tab icon.

## What's new
- Boat logo in the header, browser tab and home-screen icons.
- Tap a nearby device: choose Files, Photos and videos, or Text or link. Works between iPhone, Android and PC.
- Large files (150-500 MB) now save correctly on the receiving device.

## Names list
Open `https://YOUR-SITE/names?key=YOUR_ADMIN_KEY`. Add `&download=1` to download it.
Set `ADMIN_KEY` in Vercel: Settings, Environment Variables.

## Permanent record (required for logins to be kept forever)
Vercel's disk is read-only, so for a record that never disappears: project, Storage, Create Database, Upstash for Redis (free), connect to the project, redeploy.

Every login is saved to the database the moment it happens, and `/names?key=...` reads straight from the database, so a restart or redeploy never resets it. The first line of that page says "Storage: permanent" when the database is connected, or "NOT permanent" when it isn't.
Without the Upstash database, Vercel cannot keep anything (its disk is read-only and wiped), so this step is the only way to make it permanent.

Vercel's Redis storage gives a `REDIS_URL` connection string; Ferry reads it automatically (Upstash REST variables `KV_REST_API_URL`/`KV_REST_API_TOKEN` also work). After connecting storage, redeploy once so the new variable is picked up.
