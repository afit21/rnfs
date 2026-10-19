# file-index-node

A NomadNet page that lists the files this node shares through RNFS, each with its name, an optional
description and its CID, so visitors can fetch them with `rnfs get <CID>`.

## Install

Copy `index.mu` into NomadNet's pages directory (usually `~/.nomadnetwork/storage/pages/`) and make it
executable (`chmod +x`). The page runs the `rnfs` CLI, so the rnfs daemon must be running as the same user as
NomadNet. Edit `DESCRIPTIONS`, `HIDDEN` and `SHOW_UNNAMED` at the top of `index.mu` to suit you.

## What visitors can see

Anyone who can reach your NomadNet node can open the page. It lists a pin only if all of these are true:

- **You made it.** Its sources in `rnfs pin ls` include `user` (`rnfs add`, `rnfs pin add`, ...) or `folder`
  (your shared folder). Pins that exist only because a contact sent you a file (`app:lxmf:`, `lxmf:` before
  rnfs 0.2.0), a name you follow updated (`follow`), another node asked you to pin it (`remote:`) or an app
  pinned it (`app:`) are never listed, so the page doesn't reveal who sends you what.
- **It is complete** on this node.
- **It isn't sealed.** A sealed (encrypted) file's name is meant to stay private, so sealed files are left
  off. The page recognises one by its first bytes. It reads them once per file with `rnfs cat --raw`, which
  is a local read because the pin is complete, and it remembers the answer. If the check fails, the file is
  left off and checked again on the next view.
- **It isn't in `HIDDEN`**, and it has a name or a description (or `SHOW_UNNAMED` is `True`).

For each file the page shows only the last part of its local name (never the folders it was added from),
your description, and the CID.

Names come from whoever made the content: for a folder, its entry names are chosen by its author. Every name
and description is escaped before it is printed. Newlines, control characters and bidi overrides become
U+FFFD, so a name can't add lines to the page. Backticks and backslashes are escaped, and a leading `>`,
`<`, `#` or `-` and `#!` can't turn into a heading, comment, divider or page header. A name shows exactly as
written, on one line.

## The data directory

The page keeps its files in `~/.local/state/rnfs-pages/file-index-node/`, which is outside NomadNet's pages
directory, so none of it is served.

| File | What it is |
| --- | --- |
| `sealed.json` | Which pinned files are sealed, by CID. A CID's content never changes, so the answers stay valid. You can delete it at any time. |
| `page.mu` | The page as last rendered, served for a minute (see below). Delete it to show a change (a new pin, `DESCRIPTIONS`, `HIDDEN`) at once. |
| `lock` | Held while a request renders the page. |
| `page.log` | Errors, with details. Moved to `page.log.1` past 1 MB. |

If the daemon can't be reached, visitors only see "The file list isn't available right now". The CLI's error
message, which can contain local paths, goes to `page.log`.

`RNFS_PAGE_DIR` overrides the directory when you run the page by hand. NomadNet runs pages with a minimal
environment, so it doesn't pass that variable on.

## How often it asks rnfs

Rendering the page runs `rnfs pin ls`, and a sealed check for each file pin it hasn't checked before. So
that visitors can't keep the daemon busy by reloading the page, it renders at most once every 60 seconds
(`CACHE_SECONDS`), one request at a time (a lock, as the display page uses). Every visit in between gets the
page as last rendered, from `page.mu`. A visit that arrives while another request is rendering also gets
`page.mu`, however old, or "This page is busy" if there is none yet. The page is replaced atomically, so a
visitor never gets part of one. A failed render (the daemon isn't running) isn't kept, so the next visit
tries again.
