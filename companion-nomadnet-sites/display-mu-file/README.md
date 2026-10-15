# display-mu-file

A NomadNet page that shows an RNFS file by its CID, for example a Micron page you published with RNFS.
Visitors open it with the CID as a link variable:

```
`[My page`:/page/display-mu-file.mu`cid=bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a]
```

## Install

Copy `index.mu` into NomadNet's pages directory (usually `~/.nomadnetwork/storage/pages/`), for example as
`display-mu-file.mu`, and make it executable (`chmod +x`). The page runs the `rnfs` CLI, so the rnfs daemon
must be running as the same user as NomadNet.

## What visitors can see

Anyone who can reach your NomadNet node can open the page. The page never fetches, pins or provides anything
just because a visitor asked for it. It shows a file only if one of these is true:

- **You pinned it and this node has all of it.** The pin must come from you (source `user` or `folder` in
  `rnfs pin ls`), not from a contact, a followed name or a remote pin request. The page checks this with
  `rnfs pin ls`, which doesn't touch the network. For any other CID it runs nothing else, so it doesn't even
  fetch the root block.
- **Its CID is in your allowlist** (see below). These are the only files the page fetches from the network
  if this node doesn't have them yet.

If your node runs in `private` mode (members only), the page shows **only allowlisted CIDs**: RNFS shares your
pins with members only, and the page can't tell who is one. It asks `rnfs status` whether the node is private,
and treats an answer that doesn't say as private. Otherwise your pins are shown to anyone who knows the CID, so
don't pin files you wouldn't put on the page.

The page also refuses:

- files larger than `MAX_SIZE` (200 KB), according to `rnfs stat`;
- folders;
- sealed (encrypted) files: it reads files with `rnfs cat --raw`, so it never decrypts a file sealed to
  this node, and it refuses one it recognises;
- files that aren't UTF-8 text.

### How files are rendered

- **Allowlisted CIDs** are printed as Micron, exactly as written: links, fields and formatting work.
- **Everything else**, including your own pins that aren't on the allowlist, is shown as plain text under a
  "Third-party content" banner. The text is put in a Micron literal block, so its links, fields, headings
  and page headers (such as `#!bg=`) don't work, and control characters are replaced. This stops someone
  else's page (for example a phishing form, or one that imitates your own pages) from appearing as your
  node's content.

To show one of your own Micron pages as a page, add its CID to the allowlist.

## The data directory

The page keeps its files in `~/.local/state/rnfs-pages/display-mu-file/`, which is outside NomadNet's pages
directory, so none of it is served. The page creates the directory with mode 0700 on first use.

| File | What it is |
| --- | --- |
| `allowlist.txt` | Your allowlist. You create and edit it. |
| `page.log` | Errors, with details. Moved to `page.log.1` past 1 MB. |
| `lock` | Lets only one request run at a time. |
| `rate.json` | Times of recent requests (with the visitor's identity hash, if they identified), for the rate limits. |

NomadNet runs pages with a minimal environment. `RNFS_PAGE_DIR` overrides the directory, but it only works
when you run the page by hand, for example to test it:

```
var_cid=<CID> RNFS_PAGE_DIR=/tmp/page-test ./index.mu
```

### The allowlist

`allowlist.txt` lists one CID per line. Text after `#` is a comment, and blank lines and anything that isn't
a CID are ignored:

```
# My pages
bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a  # front page
```

An allowlisted CID that this node doesn't have yet is fetched with `rnfs cat` when a visitor opens it. That
read covers at most `MAX_SIZE` bytes, even if the file understates its size, and it pins nothing and starts
no daemon job. If it takes longer than the page's deadline (`TIMEOUT`, 120 s for the whole request), the
command is killed and nothing is left running. The fetched blocks are cached like any other read. With the
default `provide_strategy = "roots"`, this node then provides the file while it stays cached. To keep an
allowlisted file available, pin it yourself with `rnfs pin add <CID>`.

### Busy and rate limits

Only one request runs at a time. A visitor who arrives while another request is running is asked to try
again in a moment. There are also rate limits, each over any `RATE_WINDOW` (60 s):

- `VISITOR_LIMIT` (10) requests from each visitor who identified to your node. NomadNet tells the page who
  that is (`remote_identity`) only when the visitor chose to identify on the link.
- `RATE_LIMIT` (30) requests from all visitors who didn't identify, together. Nothing tells them apart:
  NomadNet's other value, `link_id`, is new with every link the visitor opens.
- `TOTAL_LIMIT` (60) requests from everyone.

So one visitor can't use up everyone else's requests. Identities cost nothing to make, though: someone who
makes several can still reach `TOTAL_LIMIT`, and then every visitor waits for the window to pass. Requests
with a malformed CID aren't counted, because they never reach rnfs. All checks fail closed: if the page can't
use its data directory, it shows nothing. You can change these numbers at the top of `index.mu`.

### The log

Visitors only see short messages such as "can't be shown right now". The details go to `page.log`: CLI
error messages (which can contain local paths), exceptions and timeouts. Check the log if the page
misbehaves.

## Upgrading from an earlier version

Earlier versions of this page pinned (source `user`) and provided every CID a visitor asked for, without
limit. Check `rnfs pin ls` for pins you didn't make, and remove them with `rnfs pin rm <CID>`.
