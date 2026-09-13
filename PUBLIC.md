# Going public

The goal this is written against: a Discord message with a snippet link in it
that unfurls into a playable card. Everything else here is what has to be true
first.

Ordered by risk rather than by interest. Stage 1 is not optional and nothing
after it is safe without it.

**Stages 1 and 2 are now built.** They are described below as what they are
rather than as what to build, because the runbook is the same either way: what
matters is knowing exactly what is reachable before you open a port. Stage 3
onward is still ahead of you.

---

## The one hard rule

**`TENMA_DEV_AUTH: "1"` must be gone before anything is reachable from outside
the LAN.** Not "before the site is finished" — before the first packet arrives.
It mints a session for any handle with no verification and makes the first
account to sign in an admin. The comment in docker-compose.yml already says
this; it is repeated here because it is the only line in this document that
cannot be undone after the fact.

The gate and dev auth are now **opposites by construction**: the gate is on
whenever dev auth is off. There is no configuration in which both are true and
no way to expose the archive by forgetting a flag — the line you have to delete
is the same line that turns the gate on. Deleting it is the whole switch.

---

## Stage 1 — a way in for you, and no way in for anybody else

Nothing is exposed in this stage. It is all local.

### What you set

Three variables in `docker-compose.yml`, under `tenma`'s `environment`:

```yaml
      # DELETE this line. Nothing below matters while it is here.
      # TENMA_DEV_AUTH: "1"

      TENMA_ADMIN_PASS: "a long passphrase you did not use anywhere else"
      TENMA_ADMIN_HANDLE: "kyabatsu"      # the handle you already sign in as
      TENMA_PUBLIC_URL: "https://your.domain"   # once you have one; see Stage 2
```

`TENMA_ADMIN_HANDLE` is not cosmetic. Authorship in this archive is by person
id, and signing in as a brand-new account would leave every note, upload and
changeset you made under dev auth attributed to a row you can no longer reach.
Set it to the handle you have been using and the existing person is **adopted**
— same id, same history, now an admin with a password. Leave it unset and you
get a fresh `admin` instead, which is fine on a database with nothing in it and
not what you want on yours.

Pick the password like a passphrase, not like a password: it is the only secret
between the internet and the archive, and the rate limit below buys you time
rather than safety.

### What that gets you

* **Deny by default**, in a middleware ahead of every route and ahead of the
  static mount. No session means refused — `/api/*`, `/media/*` and the page
  itself. The carve-out in Stage 2 is an explicit short list, not a hole
  nobody wrote down.
* **A login page** at `/`, served instead of the application. That distinction
  matters more than it sounds: handing a stranger the megabyte of SPA would
  boot it, have every one of its opening requests refused, and render as
  something *broken* rather than as something *locked*. The page is
  self-contained — no stylesheet, no webfont, no script from anywhere — because
  it renders in exactly the situation where the rest of the archive is
  unreachable.
* **A rate limit** on the login: five wrong guesses and the door stops
  answering for fifteen minutes, correct password included. Counted per caller
  **and globally**, because behind a tunnel there is only one caller —
  cloudflared dials the container from the docker network, so every request on
  earth arrives from the same address and per-IP counting would silently become
  one bucket for the whole internet.
* **Cookie flags** `HttpOnly` and `SameSite=Lax` always, and `Secure` **only
  when the request arrived over https** (read off `X-Forwarded-Proto`, which is
  what a proxy sets). Hardcoded on, the browser drops the cookie over plain
  http — which is how you reach this on the LAN today, and the symptom is a
  password that is accepted and a sign-in that never happens.
* **A sign-out** in the corner of the app, which under the gate takes you back
  to the door rather than to a read-only archive that is now all 401s.

### Unset is not open

With dev auth deleted and no password set, **nothing can sign in at all** and
only the public share surface answers. That is the same choice
`TENMA_QUARANTINE_ROOT` and `TENMA_INGEST_TOKEN` already make: a control with no
value configured refuses rather than falling back to permissive. It is also the
state the container is in for the minute between the two edits, which is why it
had to be the safe one.

The boot log says which of the three states you are in:

```
  dev auth    off
  gate        ON — sign in as kyabatsu
  ingest      enabled
```

Read that line before you open a port. `ON, but NO PASSWORD IS SET` means
nobody can get in, including you.

### The test that makes it trustworthy

`gate.mjs`. It does not check a list somebody remembered to write down — it
asks the app for **every route it registered** and walks all 83 of them with no
session, asserting each is refused. A route added in six months either appears
on the carve-out list deliberately or fails the suite. That is the difference
between a gate and a hope.

It also runs the login in a real browser, which is the one part of this that
cannot be checked by reading.

### Rotate the ingest token

`/api/ingest/*` is past the gate by design — the recorder holds
`TENMA_INGEST_TOKEN`, not a session. Which makes that token load-bearing the
moment the port is open. Rotate it at the same time, in both `docker-compose.yml`
and the Pi's `config.json`, and restart both.

---

## Stage 2 — the smallest possible public surface

Discord's unfurler is an anonymous robot. It has no session and never will. So
"only I can get in" and "Discord can show my snippets" are in direct conflict,
and the resolution is a carve-out that is as small as it can possibly be.

**Exactly three things are readable with no session:**

| Route | For |
| --- | --- |
| `GET /m/<id>` | the share card — meta tags for the robot, a player for the person |
| `GET /media/snippet/<id>` | the video itself, with range requests |
| `GET /media/snippet-poster/<id>` | the thumbnail |

and only for snippets whose status is **confirmed**. A proposed or removed one
404s to a stranger exactly as it does today — that logic already lived in
`snipVisible`, and the carve-out uses its anonymous branch rather than
bypassing it.

Note what is NOT on that list: the index, the snippets list, search, the
streams, the theater, the notes, the tools, `/media/video/*` (the masters), and
every other `/api` route. A stranger with a link to one snippet gets that
snippet and no way to enumerate a second.

### No oracle

`/m/<anything>` answers 200 with the **same page** whether the id never
existed, is not published yet, or is gated. Anything else would let somebody
paste a hundred ids and learn which hundred clips exist without ever being
allowed to see one. An unpublished moment gets "Nothing to show here" — the
same bytes a nonexistent one gets.

### The tags that make a card play

```
og:video            https://<host>/media/snippet/<id>
og:video:secure_url https://<host>/media/snippet/<id>     (only when https)
og:video:type       video/mp4
og:video:width      <w>
og:video:height     <h>
og:video:duration   <s>
```

Three things about them worth knowing, because each one is a way for this to
silently not work:

* **The type is what it will be served as, not what was uploaded.** A VP9
  upload is handed out as the normalized mp4 from the cache; an unfurler told
  `video/webm` about an mp4 plays nothing and says nothing about why.
* **`secure_url` is claimed only when the origin really is https.** Handed an
  https URL that answers on http, Discord drops the card rather than falling
  back to the plain one.
* **A picture gets no `og:video` at all**, and `og:type: website` instead of
  `video.other`. Telling an unfurler otherwise draws a play button over a PNG
  that will never play. A meme with no generated poster is now its own
  thumbnail, with a real extension on the URL, because several embedders go by
  the URL rather than the `Content-Type`.

`TENMA_PUBLIC_URL` is what makes all of those absolute URLs correct. Without
it they are built from the `Host` header, which behind a proxy is whatever the
proxy chose to pass on — as often `192.168.1.4:8080` as the name people
actually share.

### The person who clicks the card

Gets a server-rendered page: the player, the title, the length, the tags, a
save link, and a link to the rest of the archive that lands them on the door.
No application boot, no API call, nothing fetched from anywhere. Signed in, the
same URL still opens the archive at that moment, which is what you want when it
is you clicking it.

Range requests are confirmed working — `206` with a correct `Content-Range` —
which is not optional, because seeking in an unfurled player is a range request
and a server that answers one with the whole body plays from the start forever.

---

## Stage 3 — prove it for nothing, before buying anything

**Do not buy a domain yet.** Tailscale Funnel gives a public HTTPS URL on
`<machine>.<tailnet>.ts.net` with a real certificate, and you already run
tailscale. No purchase, no port forwarding, no router configuration, no
certificate to renew, and `tailscale funnel off` undoes all of it.

That is enough to answer the only question that matters:

1. Delete `TENMA_DEV_AUTH`, set `TENMA_ADMIN_PASS` and `TENMA_ADMIN_HANDLE`,
   restart, and confirm the boot log says `gate ON`.
2. Set `TENMA_PUBLIC_URL` to the `ts.net` URL and restart again.
3. `tailscale funnel 8000` on the NAS.
4. Open the URL from a phone on mobile data. You should get the door.
5. Sign in. The archive should work.
6. Paste a `/m/<id>` link into a Discord channel.

If the card plays, the proof of concept is done and you have spent nothing. If
it does not, you find out why while the whole thing is still one command from
being switched off.

**Three things worth watching in this stage:**

* **Discord caches unfurls hard.** Changing a meta tag and re-pasting the same
  link will show you the old card. Append `?v=2` — a different URL is a
  different cache entry.
* **Short snippets first.** A 200 MB ten-minute clip is at the far end of what
  an unfurled player handles well. Prove it with something under a minute
  before concluding anything about the tags.
* **`TENMA_PUBLIC_URL` has to match what you are testing.** A card built with
  the LAN address in it will unfurl to a URL Discord's robot cannot reach, and
  the failure looks exactly like the tags being wrong.

And a fourth, which is why this stage is also worth doing for its own sake: a
real HTTPS hostname is the origin test for the YouTube embed problem. If the
embeds start working through Funnel, the origin hypothesis was right.

Funnel is not the destination. It routes through Tailscale's relays and is not
built for serving a video library. It is the cheapest possible way to find out
whether the rest of the plan is worth executing.

---

## Stage 4 — the domain, once the concept is proven

A domain is a name you rent and point wherever you like. It is not a place
things live. The NAS remains the host throughout.

**Buying one.** Anywhere that sells at cost and does not upsell — Cloudflare
Registrar, Porkbun, Namecheap. A `.live` or `.net` is a few dollars a year.
The candidates already under consideration are fine; this is a preference, not
a technical decision.

**Then one of two ways to point it at the NAS**, and they differ more than
they look.

### Cloudflare Tunnel — recommended for a first site

#### What it actually is

There is no port forward anywhere in this. A small daemon on the NAS —
`cloudflared` — makes an **outbound** connection to Cloudflare and holds it
open. Public traffic arrives at Cloudflare, and Cloudflare hands it down that
existing connection. Your router never accepts an inbound connection and your
home IP never appears in DNS.

Think of it as the NAS phoning Cloudflare and staying on the line, rather than
the NAS waiting by the door.

What that buys, and each of these is a thing that would otherwise cost you an
evening:

* No inbound port. The router is untouched.
* Your home IP is never published.
* Certificates are issued and renewed for you; you never see one.
* Works behind a dynamic IP, and behind an ISP that blocks inbound 80/443.
  Both are common on residential connections.
* Revocable from a dashboard, which matters when the thing behind it is on a
  NAS holding 21 TB of irreplaceable recordings.

#### Setting it up

The whole thing is done from Cloudflare's dashboard plus one container. You do
not need the CLI.

**1. Put the domain on Cloudflare.** If you bought it at Cloudflare Registrar
it is already there. If you bought it elsewhere: add the site in Cloudflare,
and it will give you two nameservers to enter at your registrar. That change
takes minutes to hours to take effect. This step is just moving *who answers
DNS questions* about your name; nothing is pointing anywhere yet.

**2. Create the tunnel.** In the Cloudflare dashboard: **Zero Trust** →
**Networks** → **Tunnels** → **Create a tunnel** → **Cloudflared**. Name it
something like `kyabatsuNAS`. Cloudflare then shows you install commands for
various platforms — **ignore all of them** and click the **Docker** tab. What
you want out of that screen is the long token string in the command it shows.
Copy it. That token is a credential: it is how the daemon proves it is your
tunnel, so treat it like `TENMA_INGEST_TOKEN`.

**3. Add the daemon to your compose file.** In the same
`docker-compose.yml` as `tenma` and `flatfox-ml`:

```yaml
  cloudflared:
    image: cloudflare/cloudflared:latest
    container_name: cloudflared
    restart: unless-stopped
    # The token from step 2. Paste it here, or better, put it in the .env file
    # beside this one so the compose file itself is not the thing holding it.
    command: tunnel --no-autoupdate run --token ${CF_TUNNEL_TOKEN}
    networks:
      - flatfox
    # No ports. No volumes. Nothing mounted. This container's entire job is to
    # hold one outbound connection open and pass bytes down it — it has no
    # reason to touch the disk and is not given a way to.
    logging:
      driver: json-file
      options: { max-size: "10m", max-file: "3" }
```

Use whatever your `tenma` service's network is actually called. The important
part is that `cloudflared` is on the same docker network as `tenma`, because
that is how it will reach it by name.

**4. Point the tunnel at the archive.** Back in the dashboard, on your
tunnel's **Public Hostname** tab → **Add a public hostname**:

| Field | Value |
| --- | --- |
| Subdomain | blank, or `archive` |
| Domain | your domain |
| Path | blank |
| Type | `HTTP` |
| URL | `tenma:8000` |

`HTTP` and not `HTTPS` is correct and is the part that looks wrong. The leg
between Cloudflare and `cloudflared` is already encrypted — that is the tunnel.
The leg from `cloudflared` to `tenma` is inside your own docker network and
`tenma` does not speak TLS. Cloudflare terminates HTTPS for the public; the
archive keeps doing exactly what it does on the LAN.

`tenma:8000` is the container name and the **internal** port. Not the host
port you use on the LAN, and not an IP. Docker's own DNS resolves the name.

Saving this creates the DNS record for you. There is nothing to add by hand.

**5. Tell the archive its own name.** `TENMA_PUBLIC_URL: "https://your.domain"`
in `tenma`'s environment, and restart. Without it every absolute URL in the
share cards is built from a `Host` header the tunnel rewrote, and the Discord
cards will point somewhere that does not answer.

**6. Check it.** `sudo docker logs cloudflared` should show
`Registered tunnel connection` about four times — it opens several for
redundancy. The dashboard should show the tunnel **Healthy**. Then open your
domain from mobile data: the door. Sign in: the archive.

#### What to watch

* **Cloudflare's free plan is not for serving volumes of video** through their
  network. This is a real restriction, not a licensing footnote. It is fine for
  the gated UI and for unfurl cards, and it is not the answer if this ever
  serves a community's viewing. When that day comes, media moves to a hostname
  that does not proxy and the tunnel keeps everything else.
* **There is a body size limit** (100 MB on the free plan) on requests
  *through* the tunnel. That is uploads, not downloads — a clip you upload from
  a phone over the tunnel can hit it. Uploading from the LAN does not go
  through Cloudflare at all.
* **Do not also open a port.** The whole point is that there isn't one. If you
  later add a DSM reverse proxy as well, you have two front doors and only one
  of them is the one you are thinking about.

### DSM reverse proxy plus a port forward — the traditional way

Synology terminates HTTPS with a Let's Encrypt certificate it manages, and the
router forwards 443 to it.

* Full control, no third party in the path, no video restrictions.
* But: an inbound port on your home connection, your IP in public DNS, a
  dynamic-IP problem to solve, and the certificate renewal is yours to keep
  working.

If you go this way, DSM's reverse proxy has to be told to pass
`X-Forwarded-Proto` — Synology sets it by default, but confirm it, because that
header is what decides whether the session cookie is marked `Secure`.

Either way the archive itself does not change. It keeps listening on 8000 and
keeps being handed requests; only what is in front of it differs.

---

## Stage 5 — what this deliberately leaves for later

None of these block the proof of concept, and each is its own round:

* **Real accounts.** OAuth through Discord is the obvious fit for this
  audience and replaces the one-secret gate rather than extending it. The seam
  is still `identify()` in `auth.js`, and the gate does not move it.
* **Quotas and rate limits** beyond the login. Part of the auth work, not
  separate from it.
* **The Discord bot.** Once people can see snippets, `@bot` on an image to
  push it is the natural next thing — and it needs per-user upload limits
  before it exists.
* **Widening the carve-out.** Public read of the snippets list, or of the
  streams, is a decision to make deliberately and one at a time, with
  `gate.mjs` as the thing that keeps it honest: widening it means editing the
  list in two places, and one of them is a test.
* **`noindex`.** The share card asks not to be indexed, which is conservative
  while the archive is not meant to be found. It is one line in `cardPage()`
  to delete the day that changes. The unfurlers this exists for do not consult
  it either way.

---

## Unknowns worth settling early

These are cheap to check and expensive to discover late:

* Does your ISP block inbound 80 and 443? Many residential ones do. If so, the
  tunnel path is not a preference, it is the only path.
* Is your home IP static or dynamic? Dynamic is fine with a tunnel and needs
  DDNS without one.
* Is anything else on the NAS already using 443? DSM itself owns 5000/5001,
  and Synology's own reverse proxy may already be bound. Irrelevant with a
  tunnel, which is one more reason to start there.
